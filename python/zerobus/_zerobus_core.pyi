"""Type stubs for the _zerobus_core Rust module."""

from typing import Any, List, Optional, Tuple

from typing_extensions import Self

# =============================================================================
# COMMON TYPES
# =============================================================================

class RecordType:
    """Serialization format for records on a stream."""

    value: int
    PROTO: RecordType
    JSON: RecordType

    def __int__(self) -> int: ...
    def __eq__(self, other: Self) -> bool: ...
    def __repr__(self) -> str: ...

class AckCallback:
    """Base class for record acknowledgment callbacks.

    Subclass and override `on_ack` to react to server acknowledgments, and
    `on_error` to react to per-record ingestion errors.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None: ...
    def on_ack(self, offset: int) -> None: ...
    def on_error(self, offset: int, error_message: str) -> None: ...

class StreamConfigurationOptions:
    """Configuration for JSON / proto gRPC streams.

    All fields are optional kwargs. The values are applied to the Rust
    `StreamBuilder` via setters at stream-construction time.
    """

    max_inflight_records: int
    recovery: bool
    recovery_timeout_ms: int
    recovery_backoff_ms: int
    recovery_retries: int
    server_lack_of_ack_timeout_ms: int
    flush_timeout_ms: int
    record_type: RecordType
    stream_paused_max_wait_time_ms: Optional[int]
    callback_max_wait_time_ms: Optional[int]
    ack_callback: Optional[AckCallback]

    def __init__(
        self,
        *,
        max_inflight_records: int = 50_000,
        recovery: bool = True,
        recovery_timeout_ms: int = 15_000,
        recovery_backoff_ms: int = 2_000,
        recovery_retries: int = 3,
        server_lack_of_ack_timeout_ms: int = 60_000,
        flush_timeout_ms: int = 300_000,
        record_type: RecordType = ...,
        stream_paused_max_wait_time_ms: Optional[int] = None,
        callback_max_wait_time_ms: Optional[int] = 5_000,
        ack_callback: Optional[AckCallback] = None,
    ) -> None: ...
    def __repr__(self) -> str: ...

# =============================================================================
# EXCEPTIONS
# =============================================================================

class ZerobusException(Exception):
    """Base class for all Zerobus SDK exceptions."""

class NonRetriableException(ZerobusException):
    """A non-retriable error occurred."""

# =============================================================================
# AUTHENTICATION
# =============================================================================

class HeadersProvider:
    """Base class for custom authentication. Subclass and override `get_headers`."""

    def __init__(self, *args: Any, **kwargs: Any) -> None: ...
    def get_headers(self) -> List[Tuple[str, str]]: ...

# =============================================================================
# SYNC SUBMODULE
# =============================================================================

class sync:
    class ZerobusStream:
        def ingest_record_offset(self, payload: Any) -> int: ...
        def ingest_record_nowait(self, payload: Any) -> None: ...
        def ingest_records_offset(self, payloads: List[Any]) -> Optional[int]: ...
        def ingest_records_nowait(self, payloads: List[Any]) -> None: ...
        def wait_for_offset(self, offset: int) -> None: ...
        def flush(self) -> None: ...
        def close(self) -> None: ...
        def get_unacked_records(self) -> List[bytes]: ...
        def get_unacked_batches(self) -> List[List[bytes]]: ...

    class ZerobusSdk:
        def __init__(
            self,
            host: str,
            unity_catalog_url: str,
            application_name: Optional[str] = None,
        ) -> None: ...
        def create_stream(
            self,
            *,
            table: str,
            client_id: Optional[str] = None,
            client_secret: Optional[str] = None,
            headers_provider: Optional[HeadersProvider] = None,
            descriptor_bytes: Optional[bytes] = None,
            descriptor_message_name: Optional[str] = None,
            options: Optional[StreamConfigurationOptions] = None,
        ) -> sync.ZerobusStream: ...
        def recreate_stream(self, old_stream: sync.ZerobusStream) -> sync.ZerobusStream: ...
        def create_arrow_stream(
            self,
            *,
            table: str,
            schema_ipc_bytes: bytes,
            client_id: Optional[str] = None,
            client_secret: Optional[str] = None,
            headers_provider: Optional[HeadersProvider] = None,
            options: Optional["arrow.ArrowStreamConfigurationOptions"] = None,
        ) -> "arrow.ZerobusArrowStream": ...
        def recreate_arrow_stream(self, old_stream: "arrow.ZerobusArrowStream") -> "arrow.ZerobusArrowStream": ...

# =============================================================================
# ASYNC SUBMODULE
# =============================================================================

class aio:
    class ZerobusStream:
        async def ingest_record_offset(self, payload: Any) -> int: ...
        def ingest_record_nowait(self, payload: Any) -> None: ...
        async def ingest_records_offset(self, payloads: List[Any]) -> Optional[int]: ...
        def ingest_records_nowait(self, payloads: List[Any]) -> None: ...
        async def wait_for_offset(self, offset: int) -> None: ...
        async def flush(self) -> None: ...
        async def close(self) -> None: ...
        async def get_unacked_records(self) -> List[bytes]: ...
        async def get_unacked_batches(self) -> List[List[bytes]]: ...

    class ZerobusSdk:
        def __init__(
            self,
            host: str,
            unity_catalog_url: str,
            application_name: Optional[str] = None,
        ) -> None: ...
        async def create_stream(
            self,
            *,
            table: str,
            client_id: Optional[str] = None,
            client_secret: Optional[str] = None,
            headers_provider: Optional[HeadersProvider] = None,
            descriptor_bytes: Optional[bytes] = None,
            descriptor_message_name: Optional[str] = None,
            options: Optional[StreamConfigurationOptions] = None,
        ) -> aio.ZerobusStream: ...
        async def recreate_stream(self, old_stream: aio.ZerobusStream) -> aio.ZerobusStream: ...
        async def create_arrow_stream(
            self,
            *,
            table: str,
            schema_ipc_bytes: bytes,
            client_id: Optional[str] = None,
            client_secret: Optional[str] = None,
            headers_provider: Optional[HeadersProvider] = None,
            options: Optional["arrow.ArrowStreamConfigurationOptions"] = None,
        ) -> "arrow.AsyncZerobusArrowStream": ...
        async def recreate_arrow_stream(
            self, old_stream: "arrow.AsyncZerobusArrowStream"
        ) -> "arrow.AsyncZerobusArrowStream": ...

# =============================================================================
# ARROW SUBMODULE (experimental)
# =============================================================================

class arrow:
    class IPCCompression:
        NONE: arrow.IPCCompression
        LZ4_FRAME: arrow.IPCCompression
        ZSTD: arrow.IPCCompression
        def __repr__(self) -> str: ...

    class ArrowStreamConfigurationOptions:
        max_inflight_batches: int
        recovery: bool
        recovery_timeout_ms: int
        recovery_backoff_ms: int
        recovery_retries: int
        server_lack_of_ack_timeout_ms: int
        flush_timeout_ms: int
        connection_timeout_ms: int
        ipc_compression: arrow.IPCCompression
        stream_paused_max_wait_time_ms: Optional[int]

        def __init__(
            self,
            *,
            max_inflight_batches: int = 1_000,
            recovery: bool = True,
            recovery_timeout_ms: int = 15_000,
            recovery_backoff_ms: int = 2_000,
            recovery_retries: int = 4,
            server_lack_of_ack_timeout_ms: int = 60_000,
            flush_timeout_ms: int = 300_000,
            connection_timeout_ms: int = 30_000,
            ipc_compression: arrow.IPCCompression = ...,
            stream_paused_max_wait_time_ms: Optional[int] = None,
        ) -> None: ...
        def __repr__(self) -> str: ...

    class ZerobusArrowStream:
        is_closed: bool
        table_name: str

        def ingest_batch(self, ipc_bytes: bytes) -> int: ...
        def wait_for_offset(self, offset: int) -> None: ...
        def flush(self) -> None: ...
        def close(self) -> None: ...
        def get_unacked_batches(self) -> List[bytes]: ...

    class AsyncZerobusArrowStream:
        is_closed: bool
        table_name: str

        async def ingest_batch(self, ipc_bytes: bytes) -> int: ...
        async def wait_for_offset(self, offset: int) -> None: ...
        async def flush(self) -> None: ...
        async def close(self) -> None: ...
        async def get_unacked_batches(self) -> List[bytes]: ...
