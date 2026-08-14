"""
Synchronous Zerobus SDK (Rust-backed).

This module provides a high-performance synchronous interface for ingesting records
into Databricks tables via the Zerobus service. The implementation is backed by a
Rust core for optimal performance while maintaining a Pythonic API.

Example:
    >>> from zerobus.sdk.sync import ZerobusSdk, TableProperties
    >>>
    >>> sdk = ZerobusSdk(
    ...     host="https://your-shard-id.zerobus.region.cloud.databricks.com",
    ...     unity_catalog_url="https://your-workspace.cloud.databricks.com",
    ...     application_name="my-app/1.0",  # optional
    ... )
    >>>
    >>> props = TableProperties("catalog.schema.table")
    >>> stream = sdk.create_stream(
    ...     client_id="your-client-id",
    ...     client_secret="your-client-secret",
    ...     table_properties=props
    ... )
    >>>
    >>> # Optimized API - returns offset directly
    >>> offset = stream.ingest_record_offset('{"value": "record_data"}')
    >>>
    >>> # Batch API - returns one offset for the batch
    >>> batch_offset = stream.ingest_records_offset([
    ...     '{"value": "record1"}',
    ...     '{"value": "record2"}',
    ... ])
    >>>
    >>> stream.flush()  # Ensure all records are sent
    >>> stream.close()
"""

from typing import Iterator, Optional

# Import Rust-backed implementations
import zerobus._zerobus_core as _core

# Import base Rust SDK classes
_RustZerobusSdk = _core.sync.ZerobusSdk
_RustZerobusStream = _core.sync.ZerobusStream


class ZerobusStream:
    """
    Python wrapper around Rust ZerobusStream.

    Wraps the Rust implementation to provide iterator-based APIs for better
    compatibility with the old Python SDK.
    """

    def __init__(self, rust_stream: _RustZerobusStream):
        self._inner = rust_stream

    # Forward all methods to Rust, converting iterables as needed
    def ingest_record(self, payload):
        """Ingest a record and return a RecordAcknowledgment (deprecated - use ingest_record_offset)."""
        return self._inner.ingest_record(payload)

    def ingest_record_offset(self, payload):
        """Submit record and return offset immediately (no waiting).

        Returns as soon as the record is queued; the SDK sends it and tracks its
        acknowledgment in the background. The idiomatic flow is to ingest in a loop
        and call ``flush()`` once to confirm durability (or use an ``AckCallback``).
        """
        return self._inner.ingest_record_offset(payload)

    def ingest_record_nowait(self, payload):
        """Submit record without waiting (fire-and-forget).

        Spawns a detached task and discards enqueue errors. ``flush()`` can complete
        before the task allocates an offset, so this is not a safe durability path.
        Prefer ``ingest_record_offset()`` or ``ingest_records_offset()``.
        """
        return self._inner.ingest_record_nowait(payload)

    def ingest_records_offset(self, payloads):
        """Submit batch of records and return final offset."""
        return self._inner.ingest_records_offset(payloads)

    def ingest_records_nowait(self, payloads):
        """Submit batch of records without waiting.

        Same detached-task caveats as ``ingest_record_nowait()``. Prefer
        ``ingest_records_offset()``.
        """
        return self._inner.ingest_records_nowait(payloads)

    def wait_for_offset(self, offset: int):
        """Block until a specific offset is acknowledged.

        Use when you need to confirm a specific record before continuing; acks are
        ordered, so waiting on the last offset returned confirms all prior records too.
        For bulk durability, prefer ingesting in a loop and calling ``flush()`` once.
        """
        return self._inner.wait_for_offset(offset)

    def flush(self):
        """Flush all pending records, blocking until they are acknowledged.

        The idiomatic way to confirm durability: ingest in a loop, then call ``flush()``
        once to confirm everything queued so far is committed.
        """
        return self._inner.flush()

    def close(self):
        """Close the stream."""
        return self._inner.close()

    def get_unacked_records(self) -> Iterator[bytes]:
        """
        Get iterator of unacknowledged records.

        Returns:
            Iterator[bytes]: Iterator yielding record payloads that have been ingested but not yet acknowledged.
        """
        records = self._inner.get_unacked_records()
        return iter(records)

    def get_unacked_batches(self) -> Iterator[list[bytes]]:
        """
        Get iterator of unacknowledged batches.

        Returns:
            Iterator[List[bytes]]: Iterator yielding batches, where each batch is a list of record payloads.
        """
        batches = self._inner.get_unacked_batches()
        return iter(batches)

    @property
    def stream_id(self):
        """Get the stream ID (placeholder)."""
        return self._inner.stream_id if hasattr(self._inner, "stream_id") else "stream-placeholder-id"

    def get_state(self):
        """Get the current stream state (placeholder)."""
        return self._inner.get_state() if hasattr(self._inner, "get_state") else 1


class ZerobusArrowStream:
    """
    Synchronous Arrow Flight stream for ingesting pyarrow RecordBatches.

    **Beta**: Arrow Flight support is in Beta. The API is stabilising but may
    still change before reaching GA.

    Example:
        >>> import pyarrow as pa
        >>> schema = pa.schema([("temp", pa.int32())])
        >>> stream = sdk.create_arrow_stream("catalog.schema.table", schema, client_id, client_secret)
        >>> batch = pa.record_batch({"temp": [22, 23]}, schema=schema)
        >>> offset = stream.ingest_batch(batch)
        >>> stream.flush()
        >>> stream.close()
    """

    def __init__(self, rust_stream):
        self._inner = rust_stream

    def ingest_batch(self, batch) -> int:
        """
        Ingest a pyarrow.RecordBatch or pyarrow.Table.

        Args:
            batch: A pyarrow.RecordBatch or pyarrow.Table to ingest.

        Returns:
            The offset ID assigned to this batch.
        """
        from zerobus.sdk.shared.arrow import _serialize_batch

        ipc_bytes = _serialize_batch(batch)
        return self._inner.ingest_batch(ipc_bytes)

    def wait_for_offset(self, offset: int):
        """Block until a specific offset is acknowledged.

        Use when you need to confirm a specific batch before continuing; acks are
        ordered, so waiting on the last offset returned confirms all prior batches too.
        For bulk durability, prefer ingesting batches in a loop and calling ``flush()``.
        """
        return self._inner.wait_for_offset(offset)

    def flush(self):
        """Flush all pending batches, blocking until they are acknowledged.

        The idiomatic way to confirm durability: ingest batches in a loop, then call
        ``flush()`` once to confirm everything queued so far is committed.
        """
        return self._inner.flush()

    def close(self):
        """Close the stream gracefully."""
        return self._inner.close()

    @property
    def is_closed(self) -> bool:
        """Check if the stream has been closed."""
        return self._inner.is_closed

    @property
    def table_name(self) -> str:
        """Get the table name."""
        return self._inner.table_name

    def get_unacked_batches(self) -> list:
        """
        Get unacknowledged batches as a list of pyarrow.RecordBatch.

        The stream must be closed before calling this method.

        Returns:
            List of pyarrow.RecordBatch objects.
        """
        from zerobus.sdk.shared.arrow import _deserialize_batch

        ipc_list = self._inner.get_unacked_batches()
        return [_deserialize_batch(ipc_bytes) for ipc_bytes in ipc_list]


class ZerobusSdk:
    """Python wrapper around Rust ZerobusSdk that provides unified create_stream API."""

    def __init__(self, host: str, unity_catalog_url: str, application_name: Optional[str] = None):
        """
        Create a Zerobus SDK instance.

        Args:
            host: Zerobus endpoint URL
                (e.g. "https://<workspace>.zerobus.<region>.cloud.databricks.com").
            unity_catalog_url: Unity Catalog URL used for OAuth.
            application_name: Optional caller identifier (conventionally
                "<product>/<version>") appended to the HTTP user-agent header on
                gRPC requests toward the Zerobus server.
        """
        self._inner = _RustZerobusSdk(host, unity_catalog_url, application_name)

    def create_arrow_stream(
        self, table_name: str, schema, client_id: str, client_secret: str, options=None
    ) -> ZerobusArrowStream:
        """
        Create an Arrow Flight stream with OAuth client credentials.

        **Beta**: Arrow Flight support is in Beta.

        Args:
            table_name: Fully qualified table name (catalog.schema.table).
            schema: A pyarrow.Schema defining the table schema.
            client_id: OAuth client ID.
            client_secret: OAuth client secret.
            options: Optional ArrowStreamConfigurationOptions.

        Returns:
            A ZerobusArrowStream ready for ingesting RecordBatches.
        """
        from zerobus.sdk.shared.arrow import _serialize_schema

        schema_bytes = _serialize_schema(schema)
        rust_stream = self._inner.create_arrow_stream(table_name, schema_bytes, client_id, client_secret, options)
        return ZerobusArrowStream(rust_stream)

    def create_arrow_stream_with_headers_provider(
        self, table_name: str, schema, headers_provider, options=None
    ) -> ZerobusArrowStream:
        """
        Create an Arrow Flight stream with a custom headers provider.

        **Beta**: Arrow Flight support is in Beta.

        Args:
            table_name: Fully qualified table name (catalog.schema.table).
            schema: A pyarrow.Schema defining the table schema.
            headers_provider: Custom headers provider for authentication.
            options: Optional ArrowStreamConfigurationOptions.

        Returns:
            A ZerobusArrowStream ready for ingesting RecordBatches.
        """
        from zerobus.sdk.shared.arrow import _serialize_schema

        schema_bytes = _serialize_schema(schema)
        rust_stream = self._inner.create_arrow_stream_with_headers_provider(
            table_name, schema_bytes, headers_provider, options
        )
        return ZerobusArrowStream(rust_stream)

    def recreate_arrow_stream(self, old_stream: ZerobusArrowStream) -> ZerobusArrowStream:
        """
        Recreate a closed Arrow stream with the same configuration,
        re-ingesting unacknowledged batches.

        Args:
            old_stream: The closed Arrow stream to recreate.

        Returns:
            A new ZerobusArrowStream.
        """
        rust_stream = self._inner.recreate_arrow_stream(old_stream._inner)
        return ZerobusArrowStream(rust_stream)

    def create_stream(
        self,
        client_id: str,
        client_secret: str,
        table_properties,
        options=None,
        headers_provider=None,
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
            rust_stream = self._inner.create_stream_with_headers_provider(table_properties, headers_provider, options)
        else:
            # Use OAuth authentication
            rust_stream = self._inner.create_stream(client_id, client_secret, table_properties, options)
        return ZerobusStream(rust_stream)

    def recreate_stream(self, old_stream: ZerobusStream):
        """Recreate a stream from an old stream."""
        rust_stream = self._inner.recreate_stream(old_stream._inner)
        return ZerobusStream(rust_stream)


# Direct re-exports
RecordAcknowledgment = _core.sync.RecordAcknowledgment

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
    "RecordAcknowledgment",
    "TableProperties",
    "StreamConfigurationOptions",
    "RecordType",
    "AckCallback",
    "HeadersProvider",
    "ZerobusException",
    "NonRetriableException",
]
