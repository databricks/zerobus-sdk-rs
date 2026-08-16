"""
Synchronous Ingestion Example - Federated Authentication (external IdP / Entra ID)

This example demonstrates streaming to Zerobus using an external identity
provider (for example Microsoft Entra ID) instead of a Databricks OAuth
client_id/client_secret. You supply a callback that returns the current external
IdP token; the SDK exchanges it for a Zerobus-scoped Databricks token (RFC 8693
token exchange), then caches and refreshes that token for you.

Two federation modes, selected by `databricks_client_id`:
  - Account-level federation (databricks_client_id omitted): no Databricks
    service principal. The identity is synced into Databricks via Automatic
    Identity Management (SCIM).
  - Workload identity federation (databricks_client_id set): a Databricks service
    principal with a client_id and no secret, with a federation policy attached.

Record Type Mode: JSON (omitting a descriptor from TableProperties selects JSON).

Note: the existing client_id/client_secret and headers_provider authentication
paths are unchanged; `auth=FederatedToken(...)` is a new, opt-in argument.
"""

import json
import logging
import os

import requests

from zerobus import FederatedToken
from zerobus.sdk.shared import StreamConfigurationOptions, TableProperties
from zerobus.sdk.sync import ZerobusSdk

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# Configuration - update these with your values.
SERVER_ENDPOINT = os.getenv(
    "ZEROBUS_SERVER_ENDPOINT",
    "https://your-shard-id.zerobus.region.cloud.databricks.com",
)
UNITY_CATALOG_ENDPOINT = os.getenv("DATABRICKS_WORKSPACE_URL", "https://your-workspace.cloud.databricks.com")
TABLE_NAME = os.getenv("ZEROBUS_TABLE_NAME", "catalog.schema.table")

# External IdP (Entra ID) app registration used to mint the IdP token.
ENTRA_TENANT_ID = os.getenv("ENTRA_TENANT_ID", "your-entra-tenant-id")
ENTRA_CLIENT_ID = os.getenv("ENTRA_CLIENT_ID", "your-entra-client-id")
ENTRA_CLIENT_SECRET = os.getenv("ENTRA_CLIENT_SECRET", "your-entra-client-secret")
# Audience/scope your Databricks federation policy expects.
ENTRA_SCOPE = os.getenv("ENTRA_SCOPE", "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default")

# Set to the Databricks service principal for workload identity federation;
# leave unset (None) for account-level federation.
DATABRICKS_SP_CLIENT_ID = os.getenv("DATABRICKS_SP_CLIENT_ID") or None

NUM_RECORDS = 100


def get_entra_token():
    """Return a fresh external IdP (Entra ID) access token.

    This is the `idp_token_supplier` callback. The SDK calls it only when it
    needs to mint or refresh the exchanged Databricks token (a cache miss or a
    proactive refresh), not on every record, so doing a network fetch here is
    fine. A synchronous callback like this works with the sync SDK; the async
    SDK also accepts an `async def` callback.
    """
    resp = requests.post(
        f"https://login.microsoftonline.com/{ENTRA_TENANT_ID}/oauth2/v2.0/token",
        data={
            "grant_type": "client_credentials",
            "client_id": ENTRA_CLIENT_ID,
            "client_secret": ENTRA_CLIENT_SECRET,
            "scope": ENTRA_SCOPE,
        },
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def create_sample_json_record(index):
    """Create a sample record as a dict. JSON mode accepts a dict or a JSON string."""
    return {
        "device_name": f"sensor-{index % 10}",
        "temp": 20 + (index % 15),
        "humidity": 50 + (index % 40),
    }


def main():
    sdk = ZerobusSdk(SERVER_ENDPOINT, UNITY_CATALOG_ENDPOINT)

    # Opt into federated auth. Omit databricks_client_id for account-level
    # federation; set it for workload identity federation.
    auth = FederatedToken(
        idp_token_supplier=get_entra_token,
        databricks_client_id=DATABRICKS_SP_CLIENT_ID,
    )
    mode = "workload identity" if DATABRICKS_SP_CLIENT_ID else "account-level"
    logger.info("Creating stream with %s federation to %s", mode, TABLE_NAME)

    # No descriptor => JSON record format.
    table_properties = TableProperties(TABLE_NAME)
    options = StreamConfigurationOptions(max_inflight_records=100_000, recovery=True)

    stream = sdk.create_stream(table_properties=table_properties, options=options, auth=auth)
    try:
        # Queue records in a loop, then flush once. Never wait per record.
        for i in range(NUM_RECORDS):
            stream.ingest_record_offset(json.dumps(create_sample_json_record(i)))
        stream.flush()
        logger.info("Ingested and acknowledged %d records", NUM_RECORDS)
    finally:
        stream.close()


if __name__ == "__main__":
    main()
