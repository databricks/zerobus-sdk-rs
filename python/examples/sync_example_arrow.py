"""
Synchronous Arrow Flight ingestion example.

**Beta**: Arrow Flight ingestion is in Beta. The API is stabilising but may
still change before reaching GA.

Requires the `arrow` extra: ``pip install databricks-zerobus-ingest-sdk[arrow]``.
"""

import logging
import os

import pyarrow as pa

from zerobus import (
    ArrowStreamConfigurationOptions,
    IPCCompression,
    OAuth,
    ZerobusSdk,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

SERVER_ENDPOINT = os.getenv(
    "ZEROBUS_SERVER_ENDPOINT",
    "https://your-shard-id.zerobus.region.cloud.databricks.com",
)
UNITY_CATALOG_ENDPOINT = os.getenv("DATABRICKS_WORKSPACE_URL", "https://your-workspace.cloud.databricks.com")
TABLE_NAME = os.getenv("ZEROBUS_TABLE_NAME", "catalog.schema.table")
CLIENT_ID = os.getenv("DATABRICKS_CLIENT_ID", "your-oauth-client-id")
CLIENT_SECRET = os.getenv("DATABRICKS_CLIENT_SECRET", "your-oauth-client-secret")

# Arrow uses a dedicated `create_arrow_stream(table=..., schema=...)` entry
# point; no `record_format=` argument is needed.


def main():
    schema = pa.schema(
        [
            ("device_name", pa.large_utf8()),
            ("temp", pa.int32()),
            ("humidity", pa.int32()),
        ]
    )

    sdk = ZerobusSdk(
        host=SERVER_ENDPOINT,
        unity_catalog_url=UNITY_CATALOG_ENDPOINT,
        application_name="zerobus-py-arrow-example",
    )

    # Default: no IPC compression -> zero-copy ingest path is used internally
    # (Arrow IPC bytes are forwarded to the server without a parse/re-encode
    # round trip). Set `ipc_compression=IPCCompression.ZSTD` to enable
    # compression at the cost of disabling the zero-copy path.
    options = ArrowStreamConfigurationOptions(
        max_inflight_batches=100,
        ipc_compression=IPCCompression.NONE,
    )

    stream = sdk.create_arrow_stream(
        table=TABLE_NAME,
        schema=schema,
        auth=OAuth(CLIENT_ID, CLIENT_SECRET),
        options=options,
    )
    logger.info("✓ Arrow stream opened (Beta)")

    for batch_idx in range(3):
        batch = pa.record_batch(
            {
                "device_name": [f"sensor-{i % 10}" for i in range(100)],
                "temp": [20 + (i % 15) for i in range(100)],
                "humidity": [50 + (i % 40) for i in range(100)],
            },
            schema=schema,
        )
        offset = stream.ingest_batch(batch)
        logger.info("  batch %d ingested at offset %s", batch_idx, offset)

    stream.flush()
    stream.close()
    logger.info("✓ Done")


if __name__ == "__main__":
    main()
