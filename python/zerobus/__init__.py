"""
Databricks Zerobus Ingest SDK for Python.

High-performance SDK for ingesting records into Databricks tables via the Zerobus service.
This version is backed by a Rust core for optimal performance while maintaining a Pythonic API.

Example (Sync):
    >>> from zerobus import ZerobusSdk, TableProperties, AckCallback
    >>>
    >>> # Define a custom callback
    >>> class MyCallback(AckCallback):
    ...     def on_ack(self, offset):
    ...         print(f"Record acknowledged at offset {offset}")
    >>>
    >>> sdk = ZerobusSdk(
    ...     host="https://your-shard-id.zerobus.region.cloud.databricks.com",
    ...     unity_catalog_url="https://your-workspace.cloud.databricks.com"
    ... )
    >>>
    >>> props = TableProperties("catalog.schema.table")
    >>> stream = sdk.create_stream(
    ...     table_properties=props,
    ...     client_id="your-client-id",
    ...     client_secret="your-client-secret"
    ... )
    >>>
    >>> # New optimized API
    >>> offset = stream.ingest_record_offset(b"data")
    >>> stream.flush()
    >>> stream.close()

Example (Async):
    >>> import asyncio
    >>> from zerobus.sdk.aio import ZerobusSdk, TableProperties
    >>>
    >>> async def main():
    ...     sdk = ZerobusSdk(host, unity_catalog_url)
    ...     stream = await sdk.create_stream(props, client_id, client_secret)
    ...     offset = await stream.ingest_record_offset(b"data")
    ...     await stream.flush()
    ...     await stream.close()
    >>>
    >>> asyncio.run(main())
"""

# Import from Rust core
import zerobus._zerobus_core as _core
from zerobus.sdk.sync import ZerobusSdk, ZerobusStream, ZerobusArrowStream
from zerobus.sdk.shared.arrow import ArrowStreamConfigurationOptions

__version__ = "1.1.0"

# Re-export common types
TableProperties = _core.TableProperties
StreamConfigurationOptions = _core.StreamConfigurationOptions
RecordType = _core.RecordType
AckCallback = _core.AckCallback
ZerobusException = _core.ZerobusException
NonRetriableException = _core.NonRetriableException
HeadersProvider = _core.HeadersProvider
RecordAcknowledgment = _core.sync.RecordAcknowledgment

__all__ = [
    # Sync SDK (default)
    "ZerobusSdk",
    "ZerobusStream",
    # Arrow (experimental)
    "ZerobusArrowStream",
    "ArrowStreamConfigurationOptions",
    "RecordAcknowledgment",
    # Common types
    "TableProperties",
    "StreamConfigurationOptions",
    "RecordType",
    "AckCallback",
    # Authentication
    "HeadersProvider",
    # Exceptions
    "ZerobusException",
    "NonRetriableException",
    # Version
    "__version__",
]
