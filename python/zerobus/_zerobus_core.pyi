"""Type stubs for _zerobus_core Rust module."""

from typing import Any, List, Optional, Tuple, Union

from typing_extensions import Self

# =============================================================================
# COMMON TYPES
# =============================================================================

class RecordType:
    """Type of records to ingest into the stream."""

    value: int
    PROTO: RecordType
    JSON: RecordType

    def __int__(self) -> int: ...
    def __eq__(self, other: Self) -> bool: ...
    def __repr__(self) -> str: ...

class TableProperties:
    """Table properties for the stream."""

    table_name: str
    descriptor_proto: Optional[bytes]

    def __init__(self, table_name: str, descriptor_proto: Optional[Union[bytes, Any]] = None) -> None:
        """
        Create table properties.

        Args:
            table_name: Fully qualified table name (catalog.schema.table)
            descriptor_proto: Protocol buffer descriptor - can be:
                - bytes: Serialized FileDescriptorProto
                - Descriptor: Protobuf Descriptor object (e.g., MyMessage.DESCRIPTOR)
                - None: For JSON mode (no descriptor needed)
        """
        ...

    def __repr__(self) -> str: ...

class AckCallback:
    """
    Base class for record acknowledgment callbacks.

    Subclass this in Python to create custom callbacks that are invoked
    when records are acknowledged or encounter errors.

    Example:
        class MyCallback(AckCallback):
            def on_ack(self, offset: int):
                print(f"Record acknowledged at offset {offset}")

            def on_error(self, offset: int, error_message: str):
                print(f"Record at offset {offset} failed: {error_message}")
    """

    def __init__(self) -> None: ...
    def on_ack(self, offset: int) -> None:
        """
        Called when a record is acknowledged by the server.

        Args:
            offset: The offset of the acknowledged record
        """
        ...

    def on_error(self, offset: int, error_message: str) -> None:
        """
        Called when a record encounters an error.

        Args:
            offset: The offset of the failed record
            error_message: Description of the error
        """
        ...

class StreamConfigurationOptions:
    """
    Configuration options for the stream.

    All parameters are optional and will use defaults if not specified.
    """

    max_inflight_records: int
    """Maximum number of records that can be sent to the server before waiting for acknowledgment (default: 50000)"""

    recovery: bool
    """Whether to enable automatic recovery of the stream in case of failure (default: True)"""

    recovery_timeout_ms: int
    """Timeout for stream recovery in milliseconds for one attempt (default: 15000)"""

    recovery_backoff_ms: int
    """Backoff time in milliseconds between recovery attempts (default: 2000)"""

    recovery_retries: int
    """Number of retries for stream recovery (default: 3)"""

    server_lack_of_ack_timeout_ms: int
    """Number of ms in which, if we do not receive an acknowledgement, the server is considered unresponsive (default: 60000)"""

    flush_timeout_ms: int
    """Timeout for flushing the stream in milliseconds (default: 300000)"""

    record_type: RecordType
    """Type of records to ingest into the stream (default: RecordType.PROTO)"""

    stream_paused_max_wait_time_ms: Optional[int]
    """Maximum time in milliseconds to wait during graceful stream close (default: None - wait for full server duration)"""

    callback_max_wait_time_ms: Optional[int]
    """Maximum time in milliseconds to wait for callbacks to finish after calling close() (default: 5000)"""

    ack_callback: Optional[AckCallback]
    """Callback to be invoked when records are acknowledged (default: None)"""

    def __init__(
        self,
        max_inflight_records: int = 50000,
        recovery: bool = True,
        recovery_timeout_ms: int = 15000,
        recovery_backoff_ms: int = 2000,
        recovery_retries: int = 3,
        server_lack_of_ack_timeout_ms: int = 60000,
        flush_timeout_ms: int = 300000,
        record_type: RecordType = ...,
        stream_paused_max_wait_time_ms: Optional[int] = None,
        callback_max_wait_time_ms: Optional[int] = 5000,
        ack_callback: Optional[AckCallback] = None,
    ) -> None:
        """
        Create stream configuration options.

        Args:
            max_inflight_records: Maximum number of unacknowledged records (default: 50000)
            recovery: Enable automatic stream recovery (default: True)
            recovery_timeout_ms: Recovery operation timeout in ms (default: 15000)
            recovery_backoff_ms: Delay between recovery attempts in ms (default: 2000)
            recovery_retries: Maximum number of recovery attempts (default: 3)
            server_lack_of_ack_timeout_ms: Server acknowledgment timeout in ms (default: 60000)
            flush_timeout_ms: Flush operation timeout in ms (default: 300000)
            record_type: Serialization format (default: RecordType.PROTO)
            stream_paused_max_wait_time_ms: Max wait time during graceful close in ms (default: None)
            callback_max_wait_time_ms: Max wait time for callbacks after close in ms (default: 5000)
            ack_callback: Callback invoked on record acknowledgment (default: None)
        """
        ...

    def __repr__(self) -> str: ...

# =============================================================================
# EXCEPTIONS
# =============================================================================

class ZerobusException(Exception):
    """Base class for all exceptions in the Zerobus SDK."""

    ...

class NonRetriableException(ZerobusException):
    """Indicates a non-retriable error has occurred."""

    ...

# =============================================================================
# AUTHENTICATION
# =============================================================================

class HeadersProvider:
    """
    Base class for custom authentication headers (subclassable from Python).

    For custom authentication, subclass this and implement get_headers().
    For standard OAuth 2.0, use the client_id/client_secret parameters
    in create_stream() - OAuth is handled internally by the Rust SDK.
    """

    def __init__(self) -> None: ...
    def get_headers(self) -> List[Tuple[str, str]]:
        """
        Return authentication headers as a list of (key, value) tuples.

        Must include:
        - ("authorization", "Bearer <token>")
        - ("x-databricks-zerobus-table-name", "<table_name>")
        """
        ...

# =============================================================================
# SYNC SDK
# =============================================================================

class sync:
    """Synchronous Zerobus SDK."""

    class RecordAcknowledgment:
        """Future-like object for waiting on record acknowledgment."""

        def wait_for_ack(self, timeout_sec: Optional[float] = None) -> int:
            """
            Wait for the acknowledgment and return the offset ID.

            Args:
                timeout_sec: Optional timeout in seconds

            Returns:
                The offset ID of the acknowledged record

            Raises:
                RuntimeError: If called more than once
            """
            ...

        def is_done(self) -> bool:
            """Check if the acknowledgment is done."""
            ...

    class ZerobusStream:
        """Manages a single, stateful stream for ingesting records."""

        def ingest_record(self, payload: bytes | str) -> "RecordAcknowledgment":
            """
            Ingest a single record and return RecordAcknowledgment (legacy API).

            .. deprecated:: 0.3.0
                Use :meth:`ingest_record_offset` instead for better performance.
                This method returns an intermediate acknowledgment object that requires
                an additional wait_for_ack() call. The new API returns offsets directly.

            Args:
                payload: bytes (for Proto) or str (for Json)

            Returns:
                RecordAcknowledgment that can be awaited
            """
            ...

        def ingest_record_offset(self, payload: bytes | str) -> int:
            """
            Ingest a single record and return the offset directly (optimized API).

            Args:
                payload: bytes (for Proto) or str (for Json)

            Returns:
                The offset ID
            """
            ...

        def ingest_record_nowait(self, payload: bytes | str) -> None:
            """
            Ingest a single record without waiting for acknowledgment (fire-and-forget).

            Args:
                payload: bytes (for Proto) or str (for Json)
            """
            ...

        def ingest_records_offset(self, payloads: List[bytes] | List[str]) -> Optional[int]:
            """
            Ingest multiple records and return one offset for the whole batch (batch API).

            Args:
                payloads: List of bytes (for Proto) or list of str (for Json)

            Returns:
                The offset ID for the batch, or None if empty list
            """
            ...

        def ingest_records_nowait(self, payloads: List[bytes] | List[str]) -> None:
            """
            Ingest multiple records without waiting (batch fire-and-forget).

            Args:
                payloads: List of bytes (for Proto) or list of str (for Json)
            """
            ...

        def wait_for_offset(self, offset: int, timeout_sec: Optional[float] = None) -> None:
            """
            Wait for a specific offset to be acknowledged.

            Args:
                offset: The offset to wait for
                timeout_sec: Optional timeout in seconds
            """
            ...

        def flush(self) -> None:
            """Flush the stream, waiting for all pending records to be acknowledged."""
            ...

        def close(self) -> None:
            """Close the stream gracefully."""
            ...

        def get_unacked_records(self) -> int:
            """Get the number of unacknowledged records."""
            ...

    class ZerobusSdk:
        """Main entry point for synchronous Zerobus ingestion."""

        def __init__(self, host: str, unity_catalog_url: str) -> None: ...
        def set_use_tls(self, use_tls: bool) -> None:
            """
            Set whether to use TLS for connections (default: True).

            Set to False for testing with local mock servers.

            Args:
                use_tls: Whether to use TLS
            """
            ...

        def create_stream(
            self,
            table_properties: TableProperties,
            client_id: str,
            client_secret: str,
            options: Optional[StreamConfigurationOptions] = None,
        ) -> "ZerobusStream":
            """
            Create a new stream with OAuth authentication.

            Args:
                table_properties: Table properties
                client_id: OAuth client ID
                client_secret: OAuth client secret
                options: Optional configuration options

            Returns:
                A new ZerobusStream
            """
            ...

        def create_stream_with_headers_provider(
            self,
            table_properties: TableProperties,
            headers_provider: HeadersProvider,
            options: Optional[StreamConfigurationOptions] = None,
        ) -> "ZerobusStream":
            """
            Create a new stream with custom headers provider.

            Args:
                table_properties: Table properties
                headers_provider: Custom headers provider
                options: Optional configuration options

            Returns:
                A new ZerobusStream
            """
            ...

        def recreate_stream(self, old_stream: "ZerobusStream") -> "ZerobusStream":
            """
            Recreate a closed stream with the same configuration.

            Args:
                old_stream: The closed stream to recreate

            Returns:
                A new ZerobusStream
            """
            ...

# =============================================================================
# ASYNC SDK
# =============================================================================

class aio:
    """Asynchronous Zerobus SDK."""

    class ZerobusStream:
        """Manages a single, stateful stream for ingesting records (async)."""

        async def ingest_record_offset(self, payload: bytes | str) -> int:
            """
            Ingest a single record and return the offset directly.

            Args:
                payload: bytes (for Proto) or str (for Json)

            Returns:
                The offset ID
            """
            ...

        def ingest_record_nowait(self, payload: bytes | str) -> None:
            """
            Ingest a single record without waiting (fire-and-forget).

            Args:
                payload: bytes (for Proto) or str (for Json)
            """
            ...

        async def ingest_records_offset(self, payloads: List[bytes] | List[str]) -> Optional[int]:
            """
            Ingest multiple records and return one offset for the whole batch.

            Args:
                payloads: List of bytes (for Proto) or list of str (for Json)

            Returns:
                The offset ID for the batch, or None if empty list
            """
            ...

        def ingest_records_nowait(self, payloads: List[bytes] | List[str]) -> None:
            """
            Ingest multiple records without waiting (batch fire-and-forget).

            Args:
                payloads: List of bytes (for Proto) or list of str (for Json)
            """
            ...

        async def wait_for_offset(self, offset: int, timeout_sec: Optional[float] = None) -> None:
            """
            Wait for a specific offset to be acknowledged.

            Args:
                offset: The offset to wait for
                timeout_sec: Optional timeout in seconds
            """
            ...

        async def flush(self) -> None:
            """Flush the stream, waiting for all pending records."""
            ...

        async def close(self) -> None:
            """Close the stream gracefully."""
            ...

        def get_unacked_records(self) -> int:
            """Get the number of unacknowledged records."""
            ...

    class ZerobusSdk:
        """Main entry point for asynchronous Zerobus ingestion."""

        def __init__(self, host: str, unity_catalog_url: str) -> None: ...
        async def set_use_tls(self, use_tls: bool) -> None:
            """
            Set whether to use TLS for connections (default: True).

            Set to False for testing with local mock servers.

            Args:
                use_tls: Whether to use TLS
            """
            ...

        async def create_stream(
            self,
            table_properties: TableProperties,
            client_id: str,
            client_secret: str,
            options: Optional[StreamConfigurationOptions] = None,
        ) -> "ZerobusStream":
            """
            Create a new stream with OAuth authentication.

            Args:
                table_properties: Table properties
                client_id: OAuth client ID
                client_secret: OAuth client secret
                options: Optional configuration options

            Returns:
                A new ZerobusStream
            """
            ...

        async def create_stream_with_headers_provider(
            self,
            table_properties: TableProperties,
            headers_provider: HeadersProvider,
            options: Optional[StreamConfigurationOptions] = None,
        ) -> "ZerobusStream":
            """
            Create a new stream with custom headers provider.

            Args:
                table_properties: Table properties
                headers_provider: Custom headers provider
                options: Optional configuration options

            Returns:
                A new ZerobusStream
            """
            ...

        async def recreate_stream(self, old_stream: "ZerobusStream") -> "ZerobusStream":
            """
            Recreate a closed stream with the same configuration.

            Args:
                old_stream: The closed stream to recreate

            Returns:
                A new ZerobusStream
            """
            ...

# =============================================================================
# ARROW (EXPERIMENTAL)
# =============================================================================

class arrow:
    """Arrow Flight support submodule."""

    class ArrowStreamConfigurationOptions:
        """Configuration options for Arrow Flight streams."""

        max_inflight_batches: int
        """Maximum number of batches in-flight (pending acknowledgment). Default: 1000"""

        recovery: bool
        """Enable automatic stream recovery on retryable failures. Default: True"""

        recovery_timeout_ms: int
        """Timeout per recovery attempt in milliseconds. Default: 15000"""

        recovery_backoff_ms: int
        """Backoff between recovery attempts in milliseconds. Default: 2000"""

        recovery_retries: int
        """Maximum recovery retry attempts. Default: 4"""

        server_lack_of_ack_timeout_ms: int
        """Server acknowledgment timeout in milliseconds. Default: 60000"""

        flush_timeout_ms: int
        """Flush timeout in milliseconds. Default: 300000"""

        connection_timeout_ms: int
        """Connection establishment timeout in milliseconds. Default: 30000"""

        def __init__(
            self,
            *,
            max_inflight_batches: int = 1000,
            recovery: bool = True,
            recovery_timeout_ms: int = 15000,
            recovery_backoff_ms: int = 2000,
            recovery_retries: int = 4,
            server_lack_of_ack_timeout_ms: int = 60000,
            flush_timeout_ms: int = 300000,
            connection_timeout_ms: int = 30000,
        ) -> None: ...
        def __repr__(self) -> str: ...

    class ZerobusArrowStream:
        """Synchronous Arrow Flight stream for ingesting pyarrow RecordBatches."""

        @property
        def is_closed(self) -> bool: ...

        @property
        def table_name(self) -> str: ...

        def ingest_batch(self, ipc_bytes: bytes) -> int:
            """Ingest a RecordBatch (as IPC bytes) and return the logical offset."""
            ...

        def wait_for_offset(self, offset: int) -> None:
            """Wait for the server to acknowledge the batch at the given offset."""
            ...

        def flush(self) -> None:
            """Flush all pending batches, waiting for acknowledgment."""
            ...

        def close(self) -> None:
            """Close the stream gracefully."""
            ...

        def get_unacked_batches(self) -> List[bytes]:
            """Return unacknowledged batches as Arrow IPC bytes."""
            ...

    class AsyncZerobusArrowStream:
        """Asynchronous Arrow Flight stream for ingesting pyarrow RecordBatches."""

        @property
        def is_closed(self) -> bool: ...

        @property
        def table_name(self) -> str: ...

        async def ingest_batch(self, ipc_bytes: bytes) -> int:
            """Ingest a RecordBatch (as IPC bytes) and return the logical offset."""
            ...

        async def wait_for_offset(self, offset: int) -> None:
            """Wait for the server to acknowledge the batch at the given offset."""
            ...

        async def flush(self) -> None:
            """Flush all pending batches, waiting for acknowledgment."""
            ...

        async def close(self) -> None:
            """Close the stream gracefully."""
            ...

        async def get_unacked_batches(self) -> List[bytes]:
            """Return unacknowledged batches as Arrow IPC bytes."""
            ...
