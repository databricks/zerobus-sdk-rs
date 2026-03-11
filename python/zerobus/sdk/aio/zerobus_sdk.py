"""
Asynchronous Zerobus SDK (Rust-backed).

This module provides a high-performance asynchronous interface for ingesting records
into Databricks tables via the Zerobus service. The implementation is backed by a
Rust core with native async/await support for optimal performance in async applications.

Example:
    >>> import asyncio
    >>> from zerobus.sdk.aio import ZerobusSdk, TableProperties
    >>>
    >>> async def main():
    ...     sdk = ZerobusSdk(
    ...         host="https://your-shard-id.zerobus.region.cloud.databricks.com",
    ...         unity_catalog_url="https://your-workspace.cloud.databricks.com"
    ...     )
    ...
    ...     props = TableProperties("catalog.schema.table")
    ...     stream = await sdk.create_stream(
    ...         table_properties=props,
    ...         client_id="your-client-id",
    ...         client_secret="your-client-secret"
    ...     )
    ...
    ...     # Optimized async API - returns offset directly
    ...     offset = await stream.ingest_record_offset(b"record_data")
    ...     print(f"Queued at offset {offset}")
    ...
    ...     # Batch API - returns one offset for the batch
    ...     batch_offset = await stream.ingest_records_offset([b"record1", b"record2"])
    ...
    ...     # Fire-and-forget for maximum throughput
    ...     stream.ingest_record_nowait(b"record_data")  # Not awaited!
    ...     stream.ingest_records_nowait([b"record1", b"record2"])  # Not awaited!
    ...
    ...     await stream.flush()  # Ensure all records are sent
    ...     await stream.close()
    >>>
    >>> asyncio.run(main())
"""

from typing import Any, List, Optional

# Import Rust-backed implementations
import zerobus._zerobus_core as _core

# Import base Rust stream class
_RustZerobusStream = _core.aio.ZerobusStream
_RustAsyncZerobusArrowStream = _core.arrow.AsyncZerobusArrowStream


class ZerobusStream:
    """
    Python wrapper around Rust ZerobusStream that provides optimized ingest_record.

    This wrapper implements a two-stage await pattern for ingest_record:
    1. First await: Submits record quickly and returns a future
    2. Second await: Waits for acknowledgment

    This allows batching submissions without blocking on acknowledgments.
    """

    def __init__(self, rust_stream: _RustZerobusStream):
        self._inner = rust_stream

    async def ingest_record(self, payload: Any):
        """
        Ingest a single record and return a future for acknowledgment.

        This method uses a two-stage await pattern for optimal performance:
        - First await (this method): Submits the record and returns quickly with a future
        - Second await (the returned future): Waits for server acknowledgment

        Example:
            # Fast submission - queue all records quickly
            futures = []
            for record in records:
                future = await stream.ingest_record(record)  # Returns quickly!
                futures.append(future)

            # Wait for all acknowledgments concurrently
            await asyncio.gather(*futures)

        Args:
            payload: Record data (bytes, str, dict, or protobuf Message)

        Returns:
            An awaitable future that, when awaited, waits for acknowledgment and returns the offset
        """
        # Stage 1: Submit record and get offset (fast!)
        offset = await self._inner.ingest_record_offset(payload)

        # Stage 2: Return a lazy coroutine that only starts waiting when awaited
        async def wait_for_ack():
            await self._inner.wait_for_offset(offset)
            return offset

        return wait_for_ack()

    # Forward all other methods to the inner Rust stream
    async def ingest_record_offset(self, payload: Any):
        """Submit record and return offset immediately (no waiting)."""
        return await self._inner.ingest_record_offset(payload)

    def ingest_record_nowait(self, payload: Any):
        """Submit record without waiting (fire-and-forget)."""
        return self._inner.ingest_record_nowait(payload)

    async def ingest_records_offset(self, payloads):
        """Submit batch of records and return final offset."""
        return await self._inner.ingest_records_offset(payloads)

    def ingest_records_nowait(self, payloads):
        """Submit batch of records without waiting."""
        return self._inner.ingest_records_nowait(payloads)

    async def wait_for_offset(self, offset: int):
        """Wait for a specific offset to be acknowledged."""
        return await self._inner.wait_for_offset(offset)

    async def flush(self):
        """Flush all pending records."""
        return await self._inner.flush()

    async def close(self):
        """Close the stream."""
        return await self._inner.close()

    async def get_unacked_records(self):
        """
        Get iterator of unacknowledged records.

        Returns:
            Iterator[bytes]: Iterator yielding record payloads that have been ingested but not yet acknowledged.
        """
        records = await self._inner.get_unacked_records()
        return iter(records)

    async def get_unacked_batches(self):
        """
        Get iterator of unacknowledged batches.

        Returns:
            Iterator[List[bytes]]: Iterator yielding batches, where each batch is a list of record payloads.
        """
        batches = await self._inner.get_unacked_batches()
        return iter(batches)

    @property
    def stream_id(self):
        """Get the stream ID (placeholder)."""
        return "stream-placeholder-id"

    def get_state(self):
        """Get the current stream state (placeholder)."""
        return 1  # OPENED state


class ZerobusArrowStream:
    """
    Asynchronous Arrow Flight stream for ingesting pyarrow RecordBatches.

    **Experimental/Unsupported**: Arrow Flight support is experimental and not yet
    supported for production use. The API may change in future releases.
    """

    def __init__(self, rust_stream: _RustAsyncZerobusArrowStream):
        self._inner = rust_stream

    async def ingest_batch(self, batch) -> int:
        """
        Ingest a pyarrow.RecordBatch or pyarrow.Table.

        Args:
            batch: A pyarrow.RecordBatch or pyarrow.Table to ingest.

        Returns:
            The offset ID assigned to this batch.
        """
        from zerobus.sdk.shared.arrow import _serialize_batch

        ipc_bytes = _serialize_batch(batch)
        return await self._inner.ingest_batch(ipc_bytes)

    async def wait_for_offset(self, offset: int):
        """Wait for a specific offset to be acknowledged."""
        return await self._inner.wait_for_offset(offset)

    async def flush(self):
        """Flush all pending batches, waiting for acknowledgment."""
        return await self._inner.flush()

    async def close(self):
        """Close the stream gracefully."""
        return await self._inner.close()

    @property
    def is_closed(self) -> bool:
        """Check if the stream has been closed."""
        return self._inner.is_closed

    @property
    def table_name(self) -> str:
        """Get the table name."""
        return self._inner.table_name

    async def get_unacked_batches(self) -> list:
        """
        Get unacknowledged batches as a list of pyarrow.RecordBatch.

        Returns:
            List of pyarrow.RecordBatch objects.
        """
        from zerobus.sdk.shared.arrow import _deserialize_batch

        ipc_list = await self._inner.get_unacked_batches()
        return [_deserialize_batch(ipc_bytes) for ipc_bytes in ipc_list]


class ZerobusSdk:
    """Python wrapper around Rust ZerobusSdk that returns wrapped streams."""

    def __init__(self, host: str, unity_catalog_url: str):
        self._inner = _core.aio.ZerobusSdk(host, unity_catalog_url)

    async def create_arrow_stream(self, table_name: str, schema, client_id: str, client_secret: str, options=None, headers_provider=None) -> ZerobusArrowStream:
        """
        Create an Arrow Flight stream for ingesting pyarrow RecordBatches (async).

        **Experimental/Unsupported**: Arrow Flight support is experimental.

        Args:
            table_name: Fully qualified table name (catalog.schema.table).
            schema: A pyarrow.Schema defining the table schema.
            client_id: OAuth client ID.
            client_secret: OAuth client secret.
            options: Optional ArrowStreamConfigurationOptions.
            headers_provider: Optional custom headers provider.

        Returns:
            A ZerobusArrowStream ready for ingesting RecordBatches.
        """
        from zerobus.sdk.shared.arrow import _serialize_schema

        schema_bytes = _serialize_schema(schema)

        if headers_provider is not None:
            rust_stream = await self._inner.create_arrow_stream_with_headers_provider(
                table_name, schema_bytes, headers_provider, options
            )
        else:
            rust_stream = await self._inner.create_arrow_stream(
                table_name, schema_bytes, client_id, client_secret, options
            )
        return ZerobusArrowStream(rust_stream)

    async def recreate_arrow_stream(self, old_stream: ZerobusArrowStream) -> ZerobusArrowStream:
        """Recreate a closed Arrow stream with the same configuration."""
        rust_stream = await self._inner.recreate_arrow_stream(old_stream._inner)
        return ZerobusArrowStream(rust_stream)

    async def create_stream(
        self, client_id: str, client_secret: str, table_properties, options=None, headers_provider=None
    ):
        """
        Create a stream with OAuth authentication or custom headers provider.

        Args:
            client_id: OAuth client ID
            client_secret: OAuth client secret
            table_properties: Table configuration
            options: Optional stream configuration
            headers_provider: Optional custom headers provider (if set, overrides OAuth)
        """
        if headers_provider is not None:
            # Use custom headers provider (ignores client_id/client_secret)
            rust_stream = await self._inner.create_stream_with_headers_provider(
                table_properties, headers_provider, options
            )
        else:
            # Use OAuth authentication
            rust_stream = await self._inner.create_stream(client_id, client_secret, table_properties, options)
        return ZerobusStream(rust_stream)

    async def recreate_stream(self, old_stream: ZerobusStream):
        """Recreate a stream from an old stream."""
        rust_stream = await self._inner.recreate_stream(old_stream._inner)
        return ZerobusStream(rust_stream)


# Re-export common types for convenience
HeadersProvider = _core.HeadersProvider
RecordType = _core.RecordType
StreamConfigurationOptions = _core.StreamConfigurationOptions
TableProperties = _core.TableProperties
AckCallback = _core.AckCallback
ZerobusException = _core.ZerobusException
NonRetriableException = _core.NonRetriableException

__all__ = [
    "ZerobusSdk",
    "ZerobusStream",
    "ZerobusArrowStream",
    "TableProperties",
    "StreamConfigurationOptions",
    "RecordType",
    "AckCallback",
    "HeadersProvider",
    "ZerobusException",
    "NonRetriableException",
]
