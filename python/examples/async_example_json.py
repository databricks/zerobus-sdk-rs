"""
Asynchronous Ingestion Example - JSON Mode

This example demonstrates record ingestion using the asynchronous API with JSON serialization.

Record Type Mode: JSON
  - Records are sent as JSON-encoded strings
  - Omitting a descriptor from TableProperties selects JSON serialization
  - Best for dynamic schemas or when working with JSON data

Use Case: Best for applications already using asyncio, async web frameworks (FastAPI, aiohttp),
or when integrating ingestion with other asynchronous operations in an event loop.

Authentication:
  - Uses OAuth 2.0 Client Credentials (standard method)
  - Includes example of custom headers provider for advanced use cases

Note: Both sync and async APIs provide the same throughput and durability guarantees.
Choose based on your application's architecture, not performance requirements.
"""

import asyncio
import json
import logging
import os
import time

from zerobus.sdk.aio import ZerobusSdk
from zerobus.sdk.shared import (
    AckCallback,
    StreamConfigurationOptions,
    TableProperties,
)
from zerobus.sdk.shared.headers_provider import HeadersProvider

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# Configuration - update these with your values
# For AWS:
SERVER_ENDPOINT = os.getenv(
    "ZEROBUS_SERVER_ENDPOINT",
    "https://your-shard-id.zerobus.region.cloud.databricks.com",
)
UNITY_CATALOG_ENDPOINT = os.getenv("DATABRICKS_WORKSPACE_URL", "https://your-workspace.cloud.databricks.com")
# For Azure:
# SERVER_ENDPOINT = os.getenv(
#     "ZEROBUS_SERVER_ENDPOINT", "https://your-shard-id.zerobus.region.azuredatabricks.net"
# )
# UNITY_CATALOG_ENDPOINT = os.getenv(
#     "DATABRICKS_WORKSPACE_URL", "https://your-workspace.azuredatabricks.net"
# )
TABLE_NAME = os.getenv("ZEROBUS_TABLE_NAME", "catalog.schema.table")

# For OAuth authentication
CLIENT_ID = os.getenv("DATABRICKS_CLIENT_ID", "your-oauth-client-id")
CLIENT_SECRET = os.getenv("DATABRICKS_CLIENT_SECRET", "your-oauth-client-secret")

# Number of records to ingest
NUM_RECORDS = 1000


def create_sample_json_record(index):
    """
    Creates a sample AirQuality record as a dict.

    With JSON mode, you can pass either a dict or a pre-serialized JSON string.
    """
    return {
        "device_name": f"sensor-{index % 10}",
        "temp": 20 + (index % 15),
        "humidity": 50 + (index % 40),
    }


class CustomHeadersProvider(HeadersProvider):
    """
    Example custom headers provider for advanced use cases.

    Note: OAuth 2.0 Client Credentials (via create_stream()) is the standard
    authentication method. Use this only if you have specific requirements
    for custom headers (e.g., custom metadata, existing token management, etc.).
    """

    def __init__(self, custom_token: str, table_name: str):
        self.custom_token = custom_token
        self.table_name = table_name

    def get_headers(self):
        """
        Return custom headers for gRPC metadata.

        Returns:
            List of (header_name, header_value) tuples
        """
        return [
            ("authorization", f"Bearer {self.custom_token}"),
            ("x-databricks-zerobus-table-name", self.table_name),
            ("x-custom-header", "custom-value"),
        ]


class MyAckCallback(AckCallback):
    """
    Example acknowledgment callback that logs progress.

    The callback is invoked once per logical ingest submission. A batch call produces
    one callback, not one callback per record in the batch.
    """

    def __init__(self):
        super().__init__()
        self.submission_count = 0

    def on_ack(self, offset):
        """Called when a logical ingest submission is acknowledged by the server."""
        self.submission_count += 1
        # Log every 100 acknowledged submissions
        if self.submission_count % 100 == 0:
            logger.info(f"  Acknowledged up to offset: {offset} (submission #{self.submission_count})")


async def main():
    print("Starting asynchronous ingestion example (Explicit JSON Mode)...")
    print("=" * 60)

    # Check if credentials are configured
    if CLIENT_ID == "your-oauth-client-id" or CLIENT_SECRET == "your-oauth-client-secret":
        logger.error("Please set DATABRICKS_CLIENT_ID and DATABRICKS_CLIENT_SECRET environment variables")
        logger.info("Or update the CLIENT_ID and CLIENT_SECRET values in this file")
        return

    if SERVER_ENDPOINT == "https://your-shard-id.zerobus.region.cloud.databricks.com":
        logger.error("Please set ZEROBUS_SERVER_ENDPOINT environment variable")
        logger.info("Or update the SERVER_ENDPOINT value in this file")
        return

    if TABLE_NAME == "catalog.schema.table":
        logger.error("Please set ZEROBUS_TABLE_NAME environment variable")
        logger.info("Or update the TABLE_NAME value in this file")
        return

    try:
        # Step 1: Initialize the SDK
        sdk = ZerobusSdk(SERVER_ENDPOINT, UNITY_CATALOG_ENDPOINT, application_name="my-app/1.0")
        logger.info("✓ SDK initialized")

        # Step 2: Configure stream options with an ack callback
        options = StreamConfigurationOptions(
            max_inflight_records=10_000,  # Allow 10k records in flight
            recovery=True,  # Enable automatic recovery
            ack_callback=MyAckCallback(),  # Track acknowledgments
        )
        logger.info("✓ Stream configuration created")

        # Step 3: Define table properties
        # Note: No protobuf descriptor needed for JSON mode
        table_properties = TableProperties(TABLE_NAME)
        logger.info(f"✓ Table properties configured for: {TABLE_NAME} (JSON mode)")

        # Step 4: Create a stream with OAuth 2.0 authentication
        #
        # The SDK automatically:
        #   - Includes authorization header with OAuth token
        #   - Includes x-databricks-zerobus-table-name header
        stream = await sdk.create_stream(CLIENT_ID, CLIENT_SECRET, table_properties, options)

        # Advanced: Custom headers provider (for special use cases only)
        # Uncomment to use custom headers instead of OAuth:
        # custom_provider = CustomHeadersProvider("your-custom-token", TABLE_NAME)
        # stream = await sdk.create_stream(
        #     CLIENT_ID, CLIENT_SECRET, table_properties, options,
        #     headers_provider=custom_provider
        # )

        # Step 5: Ingest JSON records asynchronously
        logger.info(f"\nDemonstrating different async ingestion methods...")
        start_time = time.time()
        success_count = 0

        try:
            # ========================================================================
            # Method 1: ingest_record_offset() - Get offset for each record
            # Best for when you need individual offsets.
            # Idiomatic flow: ingest in a loop, then await flush() once (or use an
            # AckCallback) to confirm everything is durably committed.
            # ========================================================================
            logger.info("\n1. Using ingest_record_offset() - individual offsets")
            for i in range(min(10, NUM_RECORDS)):
                record_dict = create_sample_json_record(i)

                # Returns offset directly as awaitable
                offset = await stream.ingest_record_offset(record_dict)
                logger.info(f"  Record {i} ingested at offset {offset}")
                success_count += 1

            # ========================================================================
            # Method 2: ingest_records_offset() - Batch with offset
            # Best for batch processing where you need one offset for the batch
            # ========================================================================
            logger.info("\n2. Using ingest_records_offset() - batch with offset")
            batch_size = 20
            for batch_num in range(2):
                batch = []
                for i in range(batch_size):
                    idx = 10 + batch_num * batch_size + i
                    if idx >= NUM_RECORDS:
                        break
                    record_dict = create_sample_json_record(idx)
                    # Can mix dicts and JSON strings
                    if i % 2 == 0:
                        batch.append(record_dict)
                    else:
                        batch.append(json.dumps(record_dict))

                if batch:
                    batch_offset = await stream.ingest_records_offset(batch)
                    logger.info(f"  Batch {batch_num + 1}: {len(batch)} records, offset: {batch_offset}")
                    success_count += len(batch)

            # ========================================================================
            # Method 3: ingest_record_nowait() - Fire-and-forget for max throughput
            # Best for high-throughput scenarios with callback-based ack tracking
            # ========================================================================
            logger.info("\n3. Using ingest_record_nowait() - fire-and-forget")
            remaining = NUM_RECORDS - success_count
            if remaining > 0:
                for i in range(min(100, remaining)):
                    idx = success_count + i
                    record_dict = create_sample_json_record(idx)
                    stream.ingest_record_nowait(record_dict)

                logger.info(f"  Queued {min(100, remaining)} records (tracking via callback)")
                success_count += min(100, remaining)

            # ========================================================================
            # Method 4: ingest_records_nowait() - Batch fire-and-forget
            # Best for maximum throughput with batch efficiency
            # ========================================================================
            logger.info("\n4. Using ingest_records_nowait() - batch fire-and-forget")
            remaining = NUM_RECORDS - success_count
            if remaining > 0:
                batch = [create_sample_json_record(success_count + i) for i in range(remaining)]
                stream.ingest_records_nowait(batch)
                logger.info(f"  Queued {len(batch)} records in batch (tracking via callback)")
                success_count += len(batch)

            submit_end_time = time.time()
            submit_duration = submit_end_time - start_time
            logger.info(f"\n✓ All records submitted in {submit_duration:.2f} seconds")

            # Step 6: Flush and wait for all records to be durably written
            logger.info("\nFlushing stream and waiting for durability...")
            await stream.flush()
            logger.info("✓ Stream flushed (all records acknowledged)")

            end_time = time.time()
            total_duration = end_time - start_time
            records_per_second = NUM_RECORDS / total_duration
            avg_latency_ms = (total_duration * 1000.0) / NUM_RECORDS

            logger.info("✓ All records durably written")

            # Step 7: Close the stream
            await stream.close()
            logger.info("✓ Stream closed")

            # Print summary
            print("\n" + "=" * 60)
            print("Ingestion Summary:")
            print(f"  Total records: {NUM_RECORDS}")
            print(f"  Submit time: {submit_duration:.2f} seconds")
            print(f"  Total time: {total_duration:.2f} seconds")
            print(f"  Throughput: {records_per_second:.2f} records/sec")
            print(f"  Average latency: {avg_latency_ms:.2f} ms/record")
            print("  Record type: JSON")
            print("=" * 60)

        except Exception as e:
            logger.error(f"\n✗ Error during ingestion: {e}")
            await stream.close()
            raise

    except Exception as e:
        logger.error(f"\n✗ Failed to initialize stream: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
