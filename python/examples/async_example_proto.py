"""
Asynchronous Ingestion Example - Protobuf

This example demonstrates record ingestion using the asynchronous API with protobuf serialization.

Use Case: Best for applications already using asyncio, async web frameworks (FastAPI, aiohttp),
or when integrating ingestion with other asynchronous operations in an event loop.

Authentication:
  - Uses OAuth 2.0 Client Credentials (standard method)
  - Includes example of custom headers provider for advanced use cases

Note: Both sync and async APIs provide the same throughput and durability guarantees.
Choose based on your application's architecture, not performance requirements.
"""

import asyncio
import logging
import os
import time

# Import the generated protobuf module
import record_pb2

from zerobus.sdk.aio import ZerobusSdk
from zerobus.sdk.shared import (
    AckCallback,
    RecordType,
    StreamConfigurationOptions,
    TableProperties,
)
from zerobus.sdk.shared.headers_provider import HeadersProvider

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# Configuration - update these with your values
# For AWS:
SERVER_ENDPOINT = os.getenv(
    "ZEROBUS_SERVER_ENDPOINT",
    "https://your-shard-id.zerobus.region.cloud.databricks.com",
)
UNITY_CATALOG_ENDPOINT = os.getenv(
    "DATABRICKS_WORKSPACE_URL", "https://your-workspace.cloud.databricks.com"
)
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


def create_sample_record(index):
    """
    Creates a sample AirQuality record.

    You can customize this to create records with different data patterns.
    """
    return record_pb2.AirQuality(
        device_name=f"sensor-{index % 10}",
        temp=20 + (index % 15),
        humidity=50 + (index % 40),
    )


class CustomHeadersProvider(HeadersProvider):
    """
    Example custom headers provider for advanced use cases.

    Note: OAuth 2.0 Client Credentials (via create_stream()) is the standard
    authentication method. Use this only if you have specific requirements
    for custom headers (e.g., custom metadata, existing token management, etc.).
    """

    def __init__(self, custom_token: str):
        self.custom_token = custom_token

    def get_headers(self):
        """
        Return custom headers for gRPC metadata.

        Returns:
            List of (header_name, header_value) tuples
        """
        return [
            ("authorization", f"Bearer {self.custom_token}"),
            ("x-custom-header", "custom-value"),
        ]


class MyAckCallback(AckCallback):
    """
    Example acknowledgment callback that logs progress.

    The callback is invoked by the SDK whenever records are acknowledged by the server.
    """

    def __init__(self):
        super().__init__()
        self.ack_count = 0

    def on_ack(self, offset):
        """Called when records are acknowledged by the server."""
        self.ack_count += 1
        # Log every 100 acknowledgments
        if self.ack_count % 100 == 0:
            logger.info(
                f"  Acknowledged up to offset: {offset} (batch #{self.ack_count})"
            )


async def main():
    print("Starting asynchronous ingestion example (Protobuf)...")
    print("=" * 60)

    # Check if credentials are configured
    if (
        CLIENT_ID == "your-oauth-client-id"
        or CLIENT_SECRET == "your-oauth-client-secret"
    ):
        logger.error(
            "Please set DATABRICKS_CLIENT_ID and DATABRICKS_CLIENT_SECRET environment variables"
        )
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
        sdk = ZerobusSdk(SERVER_ENDPOINT, UNITY_CATALOG_ENDPOINT)
        logger.info("✓ SDK initialized")

        # Step 2: Configure stream options with protobuf record type and ack callback
        options = StreamConfigurationOptions(
            record_type=RecordType.PROTO,
            max_inflight_records=10_000,  # Allow 10k records in flight
            recovery=True,  # Enable automatic recovery
            ack_callback=MyAckCallback(),  # Track acknowledgments
        )
        logger.info("✓ Stream configuration created (Protobuf mode)")

        # Step 3: Define table properties
        # Pass the serialized FileDescriptorProto as bytes
        descriptor_bytes = record_pb2.AirQuality.DESCRIPTOR.file.serialized_pb
        table_properties = TableProperties(TABLE_NAME, descriptor_bytes)
        logger.info(f"✓ Table properties configured for: {TABLE_NAME}")

        # Step 4: Create a stream with OAuth 2.0 authentication
        #
        # The SDK automatically:
        #   - Includes authorization header with OAuth token
        #   - Includes x-databricks-zerobus-table-name header
        stream = await sdk.create_stream(
            CLIENT_ID, CLIENT_SECRET, table_properties, options
        )

        # Advanced: Custom headers provider (for special use cases only)
        # Uncomment to use custom headers instead of OAuth:
        # custom_provider = CustomHeadersProvider(custom_token="your-custom-token")
        # stream = await sdk.create_stream(
        #     CLIENT_ID, CLIENT_SECRET, table_properties, options,
        #     headers_provider=custom_provider
        # )

        # Step 5: Ingest records asynchronously
        logger.info(
            f"\nDemonstrating different async ingestion methods with protobuf..."
        )
        start_time = time.time()
        success_count = 0

        try:
            # ========================================================================
            # Method 1: ingest_record_offset() - Get offset for each record
            # ========================================================================
            logger.info("\n1. Using ingest_record_offset() - individual offsets")
            for i in range(min(10, NUM_RECORDS)):
                record = create_sample_record(i)

                # Can pass Message object directly (SDK serializes)
                offset = await stream.ingest_record_offset(record)
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
                    record = create_sample_record(idx)
                    # Can mix Message objects and pre-serialized bytes
                    if i % 2 == 0:
                        batch.append(record)
                    else:
                        batch.append(record.SerializeToString())

                if batch:
                    batch_offset = await stream.ingest_records_offset(batch)
                    logger.info(
                        f"  Batch {batch_num + 1}: {len(batch)} records, offset: {batch_offset}"
                    )
                    success_count += len(batch)

            # ========================================================================
            # Method 3: ingest_record_nowait() - Fire-and-forget
            # ========================================================================
            logger.info("\n3. Using ingest_record_nowait() - fire-and-forget")
            remaining = NUM_RECORDS - success_count
            if remaining > 0:
                for i in range(min(100, remaining)):
                    record = create_sample_record(success_count + i)
                    stream.ingest_record_nowait(record)

                logger.info(
                    f"  Queued {min(100, remaining)} records (tracking via callback)"
                )
                success_count += min(100, remaining)

            # ========================================================================
            # Method 4: ingest_records_nowait() - Batch fire-and-forget
            # ========================================================================
            logger.info("\n4. Using ingest_records_nowait() - batch fire-and-forget")
            remaining = NUM_RECORDS - success_count
            if remaining > 0:
                batch = [
                    create_sample_record(success_count + i) for i in range(remaining)
                ]
                stream.ingest_records_nowait(batch)
                logger.info(
                    f"  Queued {len(batch)} records in batch (tracking via callback)"
                )
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
            print(f"  Stream state: {stream.get_state()}")
            print("  Record type: Protobuf")
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
