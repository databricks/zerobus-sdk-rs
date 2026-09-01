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
    ...         print(f"Submission acknowledged at offset {offset}")
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
    >>> # New optimized API
    >>> offset = stream.ingest_record_offset('{"value": "data"}')
    >>> stream.flush()
    >>> stream.close()

Example (Async):
    >>> import asyncio
    >>> from zerobus.sdk.aio import ZerobusSdk, TableProperties
    >>>
    >>> async def main():
    ...     sdk = ZerobusSdk(
    ...         host="https://your-shard-id.zerobus.region.cloud.databricks.com",
    ...         unity_catalog_url="https://your-workspace.cloud.databricks.com",
    ...         application_name="my-app/1.0",
    ...     )
    ...     props = TableProperties("catalog.schema.table")
    ...     stream = await sdk.create_stream(
    ...         client_id="your-client-id",
    ...         client_secret="your-client-secret",
    ...         table_properties=props,
    ...     )
    ...     offset = await stream.ingest_record_offset('{"value": "data"}')
    ...     await stream.flush()
    ...     await stream.close()
    >>>
    >>> asyncio.run(main())
"""

# Import from Rust core
import zerobus._zerobus_core as _core
from zerobus.sdk.shared.arrow import ArrowStreamConfigurationOptions, IPCCompression
from zerobus.sdk.sync import ZerobusArrowStream, ZerobusSdk, ZerobusStream

__version__ = "1.6.1"

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
    # Arrow
    "ZerobusArrowStream",
    "ArrowStreamConfigurationOptions",
    "IPCCompression",
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
