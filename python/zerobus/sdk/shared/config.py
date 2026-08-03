"""
Configuration classes for Zerobus SDK.

This module provides documentation for configuration types exported from the Rust core.
The actual implementation is in the Rust layer, but this module adds Python-friendly
documentation and examples.
"""

# Re-export from Rust core with added documentation
from zerobus._zerobus_core import AckCallback, StreamConfigurationOptions

# Add module-level documentation
AckCallback.__doc__ = """
Base class for record acknowledgment callbacks.

Subclass this in Python to create custom callbacks that are invoked
when records are acknowledged by the server or encounter errors.

Example:
    >>> class MyCallback(AckCallback):
    ...     def on_ack(self, offset: int):
    ...         print(f"Record acknowledged at offset {offset}")
    ...
    ...     def on_error(self, offset: int, error_message: str):
    ...         print(f"Error at offset {offset}: {error_message}")
    ...
    >>> options = StreamConfigurationOptions(ack_callback=MyCallback())

Methods:
    on_ack(offset: int) -> None:
        Called when a record is successfully acknowledged by the server.

        Args:
            offset: The offset of the acknowledged record

    on_error(offset: int, error_message: str) -> None:
        Called when a record encounters an error.

        Args:
            offset: The offset of the failed record
            error_message: Description of the error
"""

StreamConfigurationOptions.__doc__ = """
Configuration options for stream behavior.

All parameters are optional and will use defaults if not specified.

Args:
    record_type: Serialization format (RecordType.PROTO or RecordType.JSON).
        Default: RecordType.PROTO
    max_inflight_records: Maximum number of records that can be sent to the
        server before waiting for acknowledgment. Default: 1000000
    recovery: Whether to enable automatic recovery of the stream in case of
        failure. Default: True
    recovery_timeout_ms: Timeout for stream recovery in milliseconds (for one
        attempt of recovery). Default: 15000
    recovery_backoff_ms: Backoff time in milliseconds between recovery attempts.
        Default: 2000
    recovery_retries: Number of retries for stream recovery. Default: 4
    server_lack_of_ack_timeout_ms: The number of ms in which, if we do not
        receive an acknowledgement, the server is considered unresponsive.
        Default: 60000
    flush_timeout_ms: Timeout for flushing the stream in milliseconds.
        Default: 300000
    stream_paused_max_wait_time_ms: Maximum time in milliseconds to wait during
        graceful stream close. When the server sends a CloseStreamSignal, the SDK
        can pause and wait for in-flight records to be acknowledged.
        - None: Wait for the full server-specified duration (most graceful)
        - 0: Immediate recovery, close stream right away
        - x: Wait up to min(x, server_duration) milliseconds
        Default: None
    callback_max_wait_time_ms: Maximum time in milliseconds to wait for callbacks
        to finish after calling close() on the stream.
        - None: Wait forever
        - x: Wait up to x milliseconds
        Default: 5000
    ack_callback: Callback to be invoked when records are acknowledged or encounter
        errors. Must be a class extending AckCallback. Default: None

Example:
    >>> from zerobus.sdk.shared import StreamConfigurationOptions, RecordType, AckCallback
    >>>
    >>> class MyCallback(AckCallback):
    ...     def on_ack(self, offset: int):
    ...         print(f"Ack: {offset}")
    ...
    >>> options = StreamConfigurationOptions(
    ...     record_type=RecordType.JSON,
    ...     max_inflight_records=10000,
    ...     recovery=True,
    ...     ack_callback=MyCallback()
    ... )
"""

__all__ = ["AckCallback", "StreamConfigurationOptions"]
