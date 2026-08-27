"""Exception mapping: ZerobusException vs NonRetriableException.

These tests hit native `map_error` without a live Zerobus server. Fatal Rust
errors (`is_retryable() == false`) must raise NonRetriableException; retryable
ones must raise the base ZerobusException, not the subclass.
"""

import asyncio
import unittest

from zerobus import (
    FederatedToken,
    HeadersProvider,
    NonRetriableException,
    StreamConfigurationOptions,
    TableProperties,
    ZerobusException,
)
from zerobus import _zerobus_core as core
from zerobus.sdk import ZerobusSdk as SyncSdk
from zerobus.sdk.aio import ZerobusSdk as AsyncSdk

HOST = "https://workspace.zerobus.cloud.databricks.com"
UC = "https://workspace.cloud.databricks.com"
# Connection refused without hanging on recovery retries.
LOCAL_REFUSED = "http://127.0.0.1:1"
NO_RECOVERY = StreamConfigurationOptions(recovery=False)


def _sync_sdk():
    return SyncSdk(HOST, UC)


class DummyHeaders(HeadersProvider):
    def get_headers(self):
        return [("authorization", "Bearer test")]


class TestExceptionHierarchy(unittest.TestCase):
    def test_non_retriable_is_subclass(self):
        self.assertTrue(issubclass(NonRetriableException, ZerobusException))

    def test_bare_zerobus_exception_catches_non_retriable(self):
        try:
            raise NonRetriableException("fatal")
        except ZerobusException as e:
            self.assertIsInstance(e, NonRetriableException)
        else:
            self.fail("NonRetriableException must be caught by except ZerobusException")


class TestNonRetriableMapping(unittest.TestCase):
    def _assert_non_retriable(self, fn):
        with self.assertRaises(NonRetriableException) as ctx:
            fn()
        self.assertIsInstance(ctx.exception, ZerobusException)

    def test_invalid_application_name_sync(self):
        self._assert_non_retriable(lambda: SyncSdk(HOST, UC, application_name="bad\nname"))

    def test_invalid_application_name_async(self):
        self._assert_non_retriable(lambda: AsyncSdk(HOST, UC, application_name="bad\nname"))

    def test_empty_table_name_on_create_stream(self):
        sdk = _sync_sdk()
        self._assert_non_retriable(lambda: sdk.create_stream("id", "secret", TableProperties(""), NO_RECOVERY))

    def test_invalid_table_name_on_create_stream(self):
        sdk = _sync_sdk()
        self._assert_non_retriable(
            lambda: sdk.create_stream("id", "secret", TableProperties("not-three-parts"), NO_RECOVERY)
        )

    def test_invalid_arrow_ipc_schema(self):
        sdk = core.sync.ZerobusSdk(HOST, UC)
        self._assert_non_retriable(lambda: sdk.create_arrow_stream("catalog.schema.table", b"not-ipc", "id", "secret"))

    def test_federated_non_string_callback_is_non_retriable(self):
        # A federated IdP callback that returns a non-string is caller misuse:
        # retrying cannot fix it, so it must be non-retriable. The supplier fails
        # during minting, before any network, so no live server is needed.
        sdk = _sync_sdk()
        self._assert_non_retriable(
            lambda: sdk.create_stream(
                table_properties=TableProperties("catalog.schema.table"),
                options=NO_RECOVERY,
                auth=FederatedToken(idp_token_supplier=lambda: 123),
            )
        )

    def test_async_empty_table_name(self):
        sdk = AsyncSdk(HOST, UC)

        async def _create():
            await sdk.create_stream("id", "secret", TableProperties(""), NO_RECOVERY)

        with self.assertRaises(NonRetriableException) as ctx:
            asyncio.run(_create())
        self.assertIsInstance(ctx.exception, ZerobusException)


class TestRetriableMapping(unittest.TestCase):
    def test_connection_refused_is_base_zerobus_exception(self):
        sdk = SyncSdk(LOCAL_REFUSED, LOCAL_REFUSED)
        with self.assertRaises(ZerobusException) as ctx:
            sdk.create_stream(
                "id",
                "secret",
                TableProperties("catalog.schema.table"),
                NO_RECOVERY,
                headers_provider=DummyHeaders(),
            )
        self.assertNotIsInstance(ctx.exception, NonRetriableException)

    def test_oauth_token_fetch_network_error_is_base_zerobus_exception(self):
        sdk = SyncSdk(HOST, LOCAL_REFUSED)
        with self.assertRaises(ZerobusException) as ctx:
            sdk.create_stream("id", "secret", TableProperties("catalog.schema.table"), NO_RECOVERY)
        self.assertNotIsInstance(ctx.exception, NonRetriableException)

    def test_federated_callback_raises_is_base_zerobus_exception(self):
        # A federated IdP callback that raises is a transient token-fetch failure
        # (e.g. the external IdP was briefly unavailable): it must be retriable,
        # like an OAuth mint error, not a fatal NonRetriableException.
        def boom():
            raise RuntimeError("idp temporarily unavailable")

        sdk = _sync_sdk()
        with self.assertRaises(ZerobusException) as ctx:
            sdk.create_stream(
                table_properties=TableProperties("catalog.schema.table"),
                options=NO_RECOVERY,
                auth=FederatedToken(idp_token_supplier=boom),
            )
        self.assertNotIsInstance(ctx.exception, NonRetriableException)
