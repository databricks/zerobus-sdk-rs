"""
Asynchronous protobuf ingestion example.
"""

import asyncio
import logging
import os
import sys
import time

sys.path.insert(0, os.path.dirname(__file__))
import record_pb2  # noqa: E402

from zerobus import Format, OAuth  # noqa: E402
from zerobus.aio import ZerobusSdk  # noqa: E402

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
    return record_pb2.AirQuality(
        device_name=f"sensor-{i % 10}",
        temp=20 + (i % 15),
        humidity=50 + (i % 40),
    )


async def main():
    sdk = ZerobusSdk(
        host=SERVER_ENDPOINT,
        unity_catalog_url=UNITY_CATALOG_ENDPOINT,
        application_name="zerobus-py-async-proto-example",
    )
    stream = await sdk.create_stream(
        table=TABLE_NAME,
        auth=OAuth(CLIENT_ID, CLIENT_SECRET),
        record_format=Format.proto(record_pb2.AirQuality.DESCRIPTOR),
    )
    logger.info("✓ Stream opened")

    start = time.time()

    offsets = await asyncio.gather(*[stream.ingest_record_offset(make_record(i)) for i in range(50)])
    logger.info("  queued 50 records, offsets %s..%s", offsets[0], offsets[-1])

    for i in range(50, NUM_RECORDS):
        stream.ingest_record_nowait(make_record(i))

    await stream.flush()
    await stream.close()
    logger.info("✓ Done in %.2fs", time.time() - start)


if __name__ == "__main__":
    asyncio.run(main())
