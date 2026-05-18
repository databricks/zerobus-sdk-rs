"""
Asynchronous Zerobus SDK (Rust-backed).

High-performance asynchronous interface (asyncio/await) for ingesting records
into Databricks tables via the Zerobus service.

Example:
    import asyncio
    from zerobus.aio import ZerobusSdk
    from zerobus import OAuth, Format

    async def main():
        sdk = ZerobusSdk(host=..., unity_catalog_url=..., application_name="my-app")
        stream = await sdk.create_stream(
            table="catalog.schema.table",
            auth=OAuth("id", "secret"),
            record_format=Format.JSON,
        )
        offset = await stream.ingest_record_offset({"k": "v"})  # dict (or str)
        await stream.flush()
        await stream.close()

    asyncio.run(main())
"""

from typing import Any, List, Optional

import zerobus._zerobus_core as _core
from zerobus.sdk.shared.auth import Auth, Headers, OAuth
from zerobus.sdk.shared.format import (
    Format,
    FormatSpec,
    _descriptor_to_bytes,
    _Json,
    _Proto,
)

_RustZerobusStream = _core.aio.ZerobusStream
_RustAsyncZerobusArrowStream = _core.arrow.AsyncZerobusArrowStream


class ZerobusStream:
    """Asynchronous gRPC ingestion stream (JSON or compiled protobuf)."""

    def __init__(self, rust_stream: _RustZerobusStream):
        self._inner = rust_stream

    async def ingest_record_offset(self, payload: Any) -> int:
        return await self._inner.ingest_record_offset(payload)

    def ingest_record_nowait(self, payload: Any) -> None:
        return self._inner.ingest_record_nowait(payload)

    async def ingest_records_offset(self, payloads: List[Any]) -> Optional[int]:
        return await self._inner.ingest_records_offset(payloads)

    def ingest_records_nowait(self, payloads: List[Any]) -> None:
        return self._inner.ingest_records_nowait(payloads)

    async def wait_for_offset(self, offset: int) -> None:
        return await self._inner.wait_for_offset(offset)

    async def flush(self) -> None:
        return await self._inner.flush()

    async def close(self) -> None:
        return await self._inner.close()

    async def get_unacked_records(self) -> List[bytes]:
        return await self._inner.get_unacked_records()

    async def get_unacked_batches(self) -> List[List[bytes]]:
        return await self._inner.get_unacked_batches()


class ZerobusArrowStream:
    """Asynchronous Arrow Flight stream for ingesting pyarrow RecordBatches.

    **Beta**: Arrow Flight ingestion is in Beta. The API is stabilising but
    may still change before reaching GA.
    """

    def __init__(self, rust_stream: _RustAsyncZerobusArrowStream):
        self._inner = rust_stream

    async def ingest_batch(self, batch) -> int:
        from zerobus.sdk.shared.arrow import _serialize_batch

        ipc_bytes = _serialize_batch(batch)
        return await self._inner.ingest_batch(ipc_bytes)

    async def wait_for_offset(self, offset: int) -> None:
        return await self._inner.wait_for_offset(offset)

    async def flush(self) -> None:
        return await self._inner.flush()

    async def close(self) -> None:
        return await self._inner.close()

    @property
    def is_closed(self) -> bool:
        return self._inner.is_closed

    @property
    def table_name(self) -> str:
        return self._inner.table_name

    async def get_unacked_batches(self) -> list:
        from zerobus.sdk.shared.arrow import _deserialize_batch

        ipc_list = await self._inner.get_unacked_batches()
        return [_deserialize_batch(ipc_bytes) for ipc_bytes in ipc_list]


def _split_auth(auth: Auth):
    if isinstance(auth, OAuth):
        return auth.client_id, auth.client_secret, None
    if isinstance(auth, Headers):
        return None, None, auth.provider
    raise TypeError(f"auth must be OAuth(...) or Headers(...); got {type(auth).__name__}")


def _split_format(fmt: FormatSpec):
    if isinstance(fmt, _Json):
        return None, None, _core.RecordType.JSON
    if isinstance(fmt, _Proto):
        descriptor_bytes, name_from_descriptor = _descriptor_to_bytes(fmt.descriptor)
        message_name = fmt.message_name or name_from_descriptor
        return descriptor_bytes, message_name, _core.RecordType.PROTO
    raise TypeError(f"record_format must be Format.JSON or Format.proto(...); got {type(fmt).__name__}")


def _options_for_record_type(
    options: Optional["_core.StreamConfigurationOptions"], record_type
) -> "_core.StreamConfigurationOptions":
    """See sync facade for rationale; `copy.copy` on PyO3 0.20 pyclasses raises."""
    if options is None:
        return _core.StreamConfigurationOptions(record_type=record_type)
    return _core.StreamConfigurationOptions(
        max_inflight_records=options.max_inflight_records,
        recovery=options.recovery,
        recovery_timeout_ms=options.recovery_timeout_ms,
        recovery_backoff_ms=options.recovery_backoff_ms,
        recovery_retries=options.recovery_retries,
        server_lack_of_ack_timeout_ms=options.server_lack_of_ack_timeout_ms,
        flush_timeout_ms=options.flush_timeout_ms,
        record_type=record_type,
        stream_paused_max_wait_time_ms=options.stream_paused_max_wait_time_ms,
        callback_max_wait_time_ms=options.callback_max_wait_time_ms,
        ack_callback=options.ack_callback,
    )


class ZerobusSdk:
    """Asynchronous Zerobus SDK handle."""

    def __init__(
        self,
        host: str,
        unity_catalog_url: str,
        *,
        application_name: Optional[str] = None,
    ):
        self._inner = _core.aio.ZerobusSdk(host, unity_catalog_url, application_name)

    async def create_stream(
        self,
        *,
        table: str,
        auth: Auth,
        record_format: FormatSpec,
        options: Optional["_core.StreamConfigurationOptions"] = None,
    ) -> ZerobusStream:
        client_id, client_secret, headers_provider = _split_auth(auth)
        descriptor_bytes, message_name, record_type = _split_format(record_format)
        effective_options = _options_for_record_type(options, record_type)

        rust_stream = await self._inner.create_stream(
            table=table,
            client_id=client_id,
            client_secret=client_secret,
            headers_provider=headers_provider,
            descriptor_bytes=descriptor_bytes,
            descriptor_message_name=message_name,
            options=effective_options,
        )
        return ZerobusStream(rust_stream)

    async def recreate_stream(self, old_stream: ZerobusStream) -> ZerobusStream:
        rust_stream = await self._inner.recreate_stream(old_stream._inner)
        return ZerobusStream(rust_stream)

    async def create_arrow_stream(
        self,
        *,
        table: str,
        schema,
        auth: Auth,
        options=None,
    ) -> ZerobusArrowStream:
        """**Beta**: Arrow Flight is in Beta — the API is stabilising but may still change before reaching GA."""
        from zerobus.sdk.shared.arrow import _serialize_schema

        client_id, client_secret, headers_provider = _split_auth(auth)
        schema_bytes = _serialize_schema(schema)
        rust_stream = await self._inner.create_arrow_stream(
            table=table,
            schema_ipc_bytes=schema_bytes,
            client_id=client_id,
            client_secret=client_secret,
            headers_provider=headers_provider,
            options=options,
        )
        return ZerobusArrowStream(rust_stream)

    async def recreate_arrow_stream(self, old_stream: ZerobusArrowStream) -> ZerobusArrowStream:
        rust_stream = await self._inner.recreate_arrow_stream(old_stream._inner)
        return ZerobusArrowStream(rust_stream)


# Re-export common types
HeadersProvider = _core.HeadersProvider
RecordType = _core.RecordType
StreamConfigurationOptions = _core.StreamConfigurationOptions
AckCallback = _core.AckCallback
ZerobusException = _core.ZerobusException
NonRetriableException = _core.NonRetriableException

__all__ = [
    "ZerobusSdk",
    "ZerobusStream",
    "ZerobusArrowStream",
    "StreamConfigurationOptions",
    "RecordType",
    "AckCallback",
    "HeadersProvider",
    "ZerobusException",
    "NonRetriableException",
    "OAuth",
    "Headers",
    "Format",
]
