"""
Databricks Zerobus Ingest SDK for Python.

High-performance SDK for ingesting records into Databricks tables via the
Zerobus service. Backed by a Rust core for performance with a Pythonic API.

Quick start (sync):
    from zerobus import ZerobusSdk, OAuth, Format

    sdk = ZerobusSdk(
        host="https://<shard>.zerobus.<region>.cloud.databricks.com",
        unity_catalog_url="https://<workspace>.cloud.databricks.com",
    )
    stream = sdk.create_stream(
        table="catalog.schema.table",
        auth=OAuth("client-id", "client-secret"),
        record_format=Format.JSON,
    )
    offset = stream.ingest_record_offset({"k": "v"})  # dict (or JSON str)
    stream.flush()
    stream.close()

Quick start (async):
    import asyncio
    from zerobus.aio import ZerobusSdk
    from zerobus import OAuth, Format

    async def main():
        sdk = ZerobusSdk(host=..., unity_catalog_url=...)
        stream = await sdk.create_stream(
            table="catalog.schema.table",
            auth=OAuth("id", "secret"),
            record_format=Format.JSON,
        )
        offset = await stream.ingest_record_offset({"k": "v"})
        await stream.flush()
        await stream.close()

    asyncio.run(main())

For Arrow Flight (Beta), use `create_arrow_stream(...)` and install the
`[arrow]` extra. See README.
"""

import zerobus._zerobus_core as _core
from zerobus.sdk.shared.arrow import ArrowStreamConfigurationOptions, IPCCompression
from zerobus.sdk.shared.auth import Auth, Headers, OAuth
from zerobus.sdk.shared.format import Format
from zerobus.sdk.sync import ZerobusArrowStream, ZerobusSdk, ZerobusStream

__version__ = "2.0.0"

# Re-export common types
StreamConfigurationOptions = _core.StreamConfigurationOptions
RecordType = _core.RecordType
AckCallback = _core.AckCallback
ZerobusException = _core.ZerobusException
NonRetriableException = _core.NonRetriableException
HeadersProvider = _core.HeadersProvider

# Note: `zerobus.aio` is a top-level shim module (`python/zerobus/aio.py`)
# loaded on `from zerobus.aio import ZerobusSdk`. It is intentionally NOT
# imported here so the attribute `zerobus.aio` resolves to a single module
# object — the shim — and not to the `zerobus.sdk.aio` sub-package.

__all__ = [
    # Sync SDK (default)
    "ZerobusSdk",
    "ZerobusStream",
    # Arrow (Beta)
    "ZerobusArrowStream",
    "ArrowStreamConfigurationOptions",
    "IPCCompression",
    # New API selectors
    "OAuth",
    "Headers",
    "Format",
    "Auth",
    # Configuration
    "StreamConfigurationOptions",
    "RecordType",
    "AckCallback",
    # Authentication base class
    "HeadersProvider",
    # Exceptions
    "ZerobusException",
    "NonRetriableException",
    # Version
    "__version__",
]
