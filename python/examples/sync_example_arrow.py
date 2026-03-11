"""
Synchronous Ingestion Example - Arrow Flight Mode

This example demonstrates record ingestion using the synchronous API with Arrow Flight.

Record Type Mode: Arrow (RecordBatch)
  - Records are sent as pyarrow RecordBatches
  - Uses Arrow Flight protocol for columnar data transfer
  - Best for structured/columnar data, DataFrames, Parquet workflows

Requirements:
  pip install databricks-zerobus-ingest-sdk[arrow]

Note: Arrow Flight support is experimental and not yet supported for production use.
"""

import logging
import os
import time

import pyarrow as pa

from zerobus.sdk.shared.arrow import ArrowStreamConfigurationOptions
from zerobus.sdk.sync import ZerobusSdk

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# Configuration - update these with your values
SERVER_ENDPOINT = os.getenv("ZEROBUS_SERVER_ENDPOINT", "https://your-shard-id.zerobus.region.cloud.databricks.com")
UNITY_CATALOG_ENDPOINT = os.getenv("DATABRICKS_WORKSPACE_URL", "https://your-workspace.cloud.databricks.com")
TABLE_NAME = os.getenv("ZEROBUS_TABLE_NAME", "catalog.schema.table")

# For OAuth authentication
CLIENT_ID = os.getenv("DATABRICKS_CLIENT_ID", "your-oauth-client-id")
CLIENT_SECRET = os.getenv("DATABRICKS_CLIENT_SECRET", "your-oauth-client-secret")

# Number of batches to ingest
NUM_BATCHES = 10
ROWS_PER_BATCH = 100

# Define the Arrow schema
SCHEMA = pa.schema(
    [
        ("device_name", pa.large_utf8()),
        ("temp", pa.int32()),
        ("humidity", pa.int64()),
    ]
)


def create_sample_batch(batch_index):
    """
    Creates a sample RecordBatch with air quality data.

    Returns a pyarrow.RecordBatch with ROWS_PER_BATCH rows.
    """
    return pa.record_batch(
        {
            "device_name": [f"sensor-{(batch_index * ROWS_PER_BATCH + i) % 10}" for i in range(ROWS_PER_BATCH)],
            "temp": [20 + ((batch_index * ROWS_PER_BATCH + i) % 15) for i in range(ROWS_PER_BATCH)],
            "humidity": [50 + ((batch_index * ROWS_PER_BATCH + i) % 40) for i in range(ROWS_PER_BATCH)],
        },
        schema=SCHEMA,
    )


def main():
    print("Starting synchronous ingestion example (Arrow Flight Mode)...")
    print("=" * 60)

    # Check if credentials are configured
    if CLIENT_ID == "your-oauth-client-id" or CLIENT_SECRET == "your-oauth-client-secret":
        logger.error("Please set DATABRICKS_CLIENT_ID and DATABRICKS_CLIENT_SECRET environment variables")
        return

    if SERVER_ENDPOINT == "https://your-shard-id.zerobus.region.cloud.databricks.com":
        logger.error("Please set ZEROBUS_SERVER_ENDPOINT environment variable")
        return

    if TABLE_NAME == "catalog.schema.table":
        logger.error("Please set ZEROBUS_TABLE_NAME environment variable")
        return

    try:
        # Step 1: Initialize the SDK
        sdk = ZerobusSdk(SERVER_ENDPOINT, UNITY_CATALOG_ENDPOINT)
        logger.info("SDK initialized")

        # Step 2: Configure arrow stream options (all optional, shown with defaults)
        options = ArrowStreamConfigurationOptions(
            max_inflight_batches=10,
            recovery=True,
            recovery_timeout_ms=15000,
            recovery_backoff_ms=2000,
            recovery_retries=3,
        )
        logger.info("Arrow stream configuration created")

        # Step 3: Create an Arrow Flight stream
        #
        # Pass a pyarrow.Schema - the SDK handles serialization internally.
        # The SDK automatically:
        #   - Includes authorization header with OAuth token
        #   - Includes x-databricks-zerobus-table-name header
        stream = sdk.create_arrow_stream(TABLE_NAME, SCHEMA, CLIENT_ID, CLIENT_SECRET, options)
        logger.info(f"Arrow stream created for table: {stream.table_name}")

        # Step 4: Ingest Arrow RecordBatches
        logger.info(f"\nIngesting {NUM_BATCHES} batches of {ROWS_PER_BATCH} rows each...")
        start_time = time.time()
        total_rows = 0

        try:
            # ========================================================================
            # Ingest RecordBatches - each call returns an offset
            # ========================================================================
            for i in range(NUM_BATCHES):
                batch = create_sample_batch(i)
                offset = stream.ingest_batch(batch)
                total_rows += batch.num_rows
                logger.info(f"  Batch {i + 1}: {batch.num_rows} rows, offset: {offset}")

            # ========================================================================
            # You can also ingest a pyarrow.Table directly
            # The SDK converts it to a single RecordBatch internally
            # ========================================================================
            table = pa.table(
                {
                    "device_name": [f"sensor-table-{i}" for i in range(50)],
                    "temp": list(range(20, 70)),
                    "humidity": list(range(50, 100)),
                },
                schema=SCHEMA,
            )
            offset = stream.ingest_batch(table)
            total_rows += table.num_rows
            logger.info(f"  Table ingested: {table.num_rows} rows, offset: {offset}")

            # ========================================================================
            # Wait for a specific offset to be acknowledged
            # ========================================================================
            logger.info(f"\nWaiting for offset {offset} to be acknowledged...")
            stream.wait_for_offset(offset)
            logger.info(f"  Offset {offset} acknowledged")

            end_time = time.time()
            duration_seconds = end_time - start_time
            rows_per_second = total_rows / duration_seconds

            # Step 5: Flush and close the stream
            logger.info("\nFlushing stream...")
            stream.flush()
            logger.info("Stream flushed")

            stream.close()
            logger.info("Stream closed")

            # Print summary
            print("\n" + "=" * 60)
            print("Ingestion Summary:")
            print(f"  Total batches: {NUM_BATCHES + 1}")
            print(f"  Total rows: {total_rows}")
            print(f"  Duration: {duration_seconds:.2f} seconds")
            print(f"  Throughput: {rows_per_second:.2f} rows/sec")
            print(f"  Record type: Arrow Flight (RecordBatch)")
            print("=" * 60)

        except Exception as e:
            logger.error(f"\nError during ingestion: {e}")

            # On failure, you can retrieve unacked batches for retry
            if stream.is_closed:
                unacked = stream.get_unacked_batches()
                if unacked:
                    logger.info(f"  {len(unacked)} unacked batches available for retry")
                    for i, batch in enumerate(unacked):
                        logger.info(f"    Batch {i}: {batch.num_rows} rows, schema: {batch.schema}")

            stream.close()
            raise

    except Exception as e:
        logger.error(f"\nFailed to initialize stream: {e}")
        raise


if __name__ == "__main__":
    main()
