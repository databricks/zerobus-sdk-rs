"""
Synchronous JSON ingestion example.

Demonstrates the v2.0.0 API:
  - `ZerobusSdk(host, unity_catalog_url, application_name=...)`
  - `sdk.create_stream(table=..., auth=OAuth(...), format=Format.JSON, options=...)`
  - keyword-only arguments and tagged-union `auth` / `format` selectors
"""

import json
import logging
import os
import time

from zerobus import Format, OAuth, StreamConfigurationOptions, ZerobusSdk

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
NUM_RECORDS = 100


def make_record(i):
    return {
        "device_name": f"sensor-{i % 10}",
        "temp": 20 + (i % 15),
        "humidity": 50 + (i % 40),
    }


def main():
    sdk = ZerobusSdk(
        host=SERVER_ENDPOINT,
        unity_catalog_url=UNITY_CATALOG_ENDPOINT,
        application_name="zerobus-py-json-example",
    )
    stream = sdk.create_stream(
        table=TABLE_NAME,
        auth=OAuth(CLIENT_ID, CLIENT_SECRET),
        record_format=Format.JSON,
        options=StreamConfigurationOptions(max_inflight_records=1000),
    )
    logger.info("✓ Stream opened")

    start = time.time()

    # 1) Single record with offset
    for i in range(10):
        offset = stream.ingest_record_offset(make_record(i))
        logger.info("  record %d -> offset %s", i, offset)

    # 2) Batch with one offset for the whole batch (mix dict + JSON string)
    batch = [make_record(i) if i % 2 == 0 else json.dumps(make_record(i)) for i in range(10, 30)]
    batch_offset = stream.ingest_records_offset(batch)
    logger.info("  batch of %d -> offset %s", len(batch), batch_offset)

    # 3) Fire-and-forget for max throughput
    for i in range(30, NUM_RECORDS):
        stream.ingest_record_nowait(make_record(i))

    stream.flush()
    stream.close()

    elapsed = time.time() - start
    logger.info("✓ Done: %d records in %.2fs (%.0f rps)", NUM_RECORDS, elapsed, NUM_RECORDS / elapsed)


if __name__ == "__main__":
    main()
