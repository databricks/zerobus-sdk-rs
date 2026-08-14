"""
Synchronous Ingestion Example - JSON Mode

This example demonstrates record ingestion using the synchronous API with JSON serialization.

Record Type Mode: JSON
  - Records are sent as JSON-encoded strings
  - Omitting a descriptor from TableProperties selects JSON serialization
  - Best for dynamic schemas or when working with JSON data

Authentication:
  - Uses OAuth 2.0 Client Credentials (standard method)
  - Includes example of custom headers provider for advanced use cases

Note: Both sync and async APIs provide the same throughput and durability guarantees.
Choose based on your application's architecture, not performance requirements.
"""

import json
import logging
import os
import time

from zerobus.sdk.shared import StreamConfigurationOptions, TableProperties
from zerobus.sdk.shared.headers_provider import HeadersProvider
from zerobus.sdk.sync import ZerobusSdk

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
NUM_RECORDS = 100


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


def main():
    print("Starting synchronous ingestion example (JSON)...")
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

        # Step 2: Define table properties
        # Note: No protobuf descriptor needed for JSON mode
        table_properties = TableProperties(TABLE_NAME)
        logger.info(f"✓ Table properties configured for: {TABLE_NAME} (JSON mode)")

        # Step 3: Create stream configuration
        options = StreamConfigurationOptions(
            max_inflight_records=1000,
            recovery=True,
            recovery_timeout_ms=15000,
            recovery_backoff_ms=2000,
            recovery_retries=4,
        )
        logger.info("✓ Stream configuration created")

        # Step 4: Create a stream with OAuth 2.0 authentication
        #
        # The SDK automatically:
        #   - Includes authorization header with OAuth token
        #   - Includes x-databricks-zerobus-table-name header
        stream = sdk.create_stream(CLIENT_ID, CLIENT_SECRET, table_properties, options)

        # Advanced: Custom headers provider (for special use cases only)
        # Uncomment to use custom headers instead of OAuth:
        # custom_provider = CustomHeadersProvider("your-custom-token", TABLE_NAME)
        # stream = sdk.create_stream(
        #     CLIENT_ID, CLIENT_SECRET, table_properties, options,
        #     headers_provider=custom_provider
        # )

        # Step 5: Ingest JSON records synchronously
        logger.info(f"\nDemonstrating different ingestion methods...")
        start_time = time.time()
        success_count = 0

        try:
            # ========================================================================
            # Method 1: ingest_record_offset() - RECOMMENDED for single records
            # Returns offset directly without intermediate acknowledgment object.
            # Idiomatic flow: ingest in a loop, then flush() once at the end (see below)
            # to confirm everything is durably committed.
            # ========================================================================
            logger.info("\n1. Using ingest_record_offset() - optimized API")
            for i in range(min(10, NUM_RECORDS)):
                record_dict = create_sample_json_record(i)

                # Returns offset directly - more efficient than ingest_record()
                offset = stream.ingest_record_offset(record_dict)
                logger.info(f"  Record {i} ingested at offset {offset}")
                success_count += 1

            # ========================================================================
            # Method 2: ingest_records_offset() - RECOMMENDED for batch ingestion
            # Ingests multiple records and returns one offset for the whole batch
            # ========================================================================
            logger.info("\n2. Using ingest_records_offset() - batch API")
            batch_size = 20
            for batch_num in range(2):
                batch = []
                for i in range(batch_size):
                    idx = 10 + batch_num * batch_size + i
                    if idx >= NUM_RECORDS:
                        break
                    record_dict = create_sample_json_record(idx)
                    # Can mix dicts and JSON strings in the same batch
                    if i % 2 == 0:
                        batch.append(record_dict)
                    else:
                        batch.append(json.dumps(record_dict))

                if batch:
                    # Returns one offset for the whole batch
                    batch_offset = stream.ingest_records_offset(batch)
                    logger.info(f"  Batch {batch_num + 1}: {len(batch)} records, offset: {batch_offset}")
                    success_count += len(batch)

            remaining = NUM_RECORDS - success_count
            if remaining > 0:
                batch = [create_sample_json_record(success_count + i) for i in range(remaining)]
                batch_offset = stream.ingest_records_offset(batch)
                logger.info(f"  Remaining {len(batch)} records, offset: {batch_offset}")
                success_count += len(batch)

            # ========================================================================
            # Legacy Method: ingest_record() - DEPRECATED
            # Returns acknowledgment object that needs separate wait_for_ack() call
            # Use ingest_record_offset() instead
            # ========================================================================
            # ack = stream.ingest_record(record_dict)  # Deprecated
            # offset = ack.wait_for_ack()  # Extra step needed

            # Step 6: Flush and close the stream
            logger.info("\nFlushing stream...")
            stream.flush()
            logger.info("✓ Stream flushed")

            end_time = time.time()
            duration_seconds = end_time - start_time
            records_per_second = NUM_RECORDS / duration_seconds

            stream.close()
            logger.info("✓ Stream closed")

            # Print summary
            print("\n" + "=" * 60)
            print("Ingestion Summary:")
            print(f"  Total records: {NUM_RECORDS}")
            print(f"  Successful: {success_count}")
            print(f"  Failed: {NUM_RECORDS - success_count}")
            print(f"  Duration: {duration_seconds:.2f} seconds")
            print(f"  Throughput: {records_per_second:.2f} records/sec")
            print("  Record type: JSON")
            print("=" * 60)

        except Exception as e:
            logger.error(f"\n✗ Error during ingestion: {e}")
            stream.close()
            raise

    except Exception as e:
        logger.error(f"\n✗ Failed to initialize stream: {e}")
        raise


if __name__ == "__main__":
    main()
