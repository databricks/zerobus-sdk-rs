"""
Synchronous protobuf ingestion example.

Demonstrates the v2.0.0 API with protobuf records. Also shows a custom
HeadersProvider for non-standard authentication scenarios.
"""

import logging
import os
import sys
import time

# Make `record_pb2` importable when running this file directly.
sys.path.insert(0, os.path.dirname(__file__))
import record_pb2  # noqa: E402

from zerobus import Format, HeadersProvider, OAuth, ZerobusSdk  # noqa: E402

# `from zerobus import Headers` is what you would import to use a custom
# HeadersProvider with `auth=Headers(provider)`; see the commented snippet
# in `main()` below.

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


class CustomHeadersProvider(HeadersProvider):
    """Example: bring-your-own token instead of OAuth client credentials."""

    def __init__(self, token: str):
        super().__init__()
        self._token = token

    def get_headers(self):
        return [("authorization", f"Bearer {self._token}")]


def make_record(i):
    return record_pb2.AirQuality(
        device_name=f"sensor-{i % 10}",
        temp=20 + (i % 15),
        humidity=50 + (i % 40),
    )


def main():
    sdk = ZerobusSdk(
        host=SERVER_ENDPOINT,
        unity_catalog_url=UNITY_CATALOG_ENDPOINT,
        application_name="zerobus-py-proto-example",
    )

    # OAuth is the default. To use a custom HeadersProvider, swap in:
    #   from zerobus import Headers
    #   auth=Headers(CustomHeadersProvider("your-token"))
    auth = OAuth(CLIENT_ID, CLIENT_SECRET)

    stream = sdk.create_stream(
        table=TABLE_NAME,
        auth=auth,
        record_format=Format.proto(record_pb2.AirQuality.DESCRIPTOR),
    )
    logger.info("✓ Stream opened")

    start = time.time()

    for i in range(10):
        offset = stream.ingest_record_offset(make_record(i))
        logger.info("  record %d -> offset %s", i, offset)

    batch = [make_record(i) for i in range(10, 30)]
    batch_offset = stream.ingest_records_offset(batch)
    logger.info("  batch of %d -> offset %s", len(batch), batch_offset)

    for i in range(30, NUM_RECORDS):
        stream.ingest_record_nowait(make_record(i))

    stream.flush()
    stream.close()

    elapsed = time.time() - start
    logger.info("✓ Done: %d records in %.2fs (%.0f rps)", NUM_RECORDS, elapsed, NUM_RECORDS / elapsed)


if __name__ == "__main__":
    main()
