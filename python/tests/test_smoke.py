"""
Smoke tests for the Rust-backed Python SDK.

These tests verify that the Python API surface is correct without making
actual network connections. Integration testing is covered by the Rust SDK's
comprehensive test suite.
"""

import unittest

from zerobus import (
    AckCallback,
    HeadersProvider,
    NonRetriableException,
    RecordType,
    StreamConfigurationOptions,
    TableProperties,
    ZerobusException,
)
from zerobus.sdk import ZerobusSdk as SyncSdk
from zerobus.sdk.aio import ZerobusSdk as AsyncSdk


class TestImports(unittest.TestCase):
    """Test that all public API classes can be imported."""

    def test_import_exceptions(self):
        """Test exception classes are available."""
        self.assertTrue(issubclass(ZerobusException, Exception))
        self.assertTrue(issubclass(NonRetriableException, Exception))

    def test_import_record_type(self):
        """Test RecordType enum is available."""
        self.assertEqual(RecordType.PROTO.value, 1)
        self.assertEqual(RecordType.JSON.value, 2)

    def test_import_from_shared(self):
        """Test that recommended import pattern from zerobus.sdk.shared works."""
        from zerobus.sdk.shared import (
            HeadersProvider,
            RecordType,
            StreamConfigurationOptions,
            TableProperties,
        )

        # Verify imports work
        self.assertEqual(RecordType.PROTO.value, 1)
        self.assertIsNotNone(StreamConfigurationOptions)
        self.assertIsNotNone(TableProperties)
        self.assertIsNotNone(HeadersProvider)

    def test_import_from_headers_provider_module(self):
        """Test that backward-compatible import from headers_provider module works."""
        from zerobus.sdk.shared.headers_provider import HeadersProvider

        # Verify imports work
        self.assertIsNotNone(HeadersProvider)

    def test_import_configuration(self):
        """Test configuration classes are available."""
        options = StreamConfigurationOptions(
            max_inflight_records=100,
            recovery=True,
            record_type=RecordType.JSON,
        )
        self.assertEqual(options.max_inflight_records, 100)
        self.assertTrue(options.recovery)
        self.assertEqual(options.record_type, RecordType.JSON)

    def test_import_table_properties(self):
        """Test TableProperties can be created."""
        props = TableProperties("catalog.schema.table")
        self.assertEqual(props.table_name, "catalog.schema.table")

    def test_import_callbacks(self):
        """Test callback classes are available."""

        class TestCallback(AckCallback):
            def on_ack(self, offset: int):
                pass

        callback = TestCallback()
        self.assertIsInstance(callback, AckCallback)

    def test_import_headers_provider(self):
        """Test HeadersProvider classes are available."""

        class CustomProvider(HeadersProvider):
            def get_headers(self):
                return [("authorization", "Bearer test")]

        provider = CustomProvider()
        self.assertIsInstance(provider, HeadersProvider)
        self.assertEqual(provider.get_headers(), [("authorization", "Bearer test")])


class TestSDKAPISurface(unittest.TestCase):
    """Test that SDK classes have the expected API surface."""

    def test_sync_sdk_has_required_methods(self):
        """Test sync SDK has all required methods."""
        # Don't actually connect - just verify the class has the right methods
        self.assertTrue(hasattr(SyncSdk, "__init__"))
        self.assertTrue(hasattr(SyncSdk, "create_stream"))
        self.assertTrue(hasattr(SyncSdk, "recreate_stream"))

    def test_async_sdk_has_required_methods(self):
        """Test async SDK has all required methods."""
        self.assertTrue(hasattr(AsyncSdk, "__init__"))
        self.assertTrue(hasattr(AsyncSdk, "create_stream"))
        self.assertTrue(hasattr(AsyncSdk, "recreate_stream"))


class TestStreamAPISurface(unittest.TestCase):
    """Test that Stream classes have the expected API surface."""

    def test_stream_methods_exist(self):
        """Test that stream has all expected methods."""
        from zerobus import _zerobus_core

        # Methods common to both sync and async streams
        common_methods = [
            "ingest_record_offset",
            "ingest_record_nowait",
            "ingest_records_offset",
            "ingest_records_nowait",
            "wait_for_offset",
            "flush",
            "close",
            "get_unacked_records",
        ]

        # Check sync stream (includes deprecated method)
        sync_stream_class = _zerobus_core.sync.ZerobusStream
        sync_methods = common_methods + ["ingest_record"]  # Sync has deprecated method
        for method in sync_methods:
            self.assertTrue(
                hasattr(sync_stream_class, method),
                f"Sync stream missing method: {method}",
            )

        # Check async stream (no deprecated method)
        async_stream_class = _zerobus_core.aio.ZerobusStream
        for method in common_methods:
            self.assertTrue(
                hasattr(async_stream_class, method),
                f"Async stream missing method: {method}",
            )

        # Verify async also has deprecated method (for backwards compatibility)
        self.assertTrue(
            hasattr(async_stream_class, "ingest_record"),
            "Async stream should have deprecated ingest_record method",
        )


class TestDeprecations(unittest.TestCase):
    """Test that deprecated APIs are properly marked."""

    def test_ingest_record_is_deprecated(self):
        """Verify that ingest_record method exists but is marked deprecated."""
        # The method should still exist for backwards compatibility
        from zerobus import _zerobus_core

        # Both sync and async should have the method
        self.assertTrue(hasattr(_zerobus_core.sync.ZerobusStream, "ingest_record"))
        self.assertTrue(hasattr(_zerobus_core.aio.ZerobusStream, "ingest_record"))

        # Both have the deprecated method for backwards compatibility


class TestRecordTypeConstants(unittest.TestCase):
    """Test RecordType enum values."""

    def test_proto_constant(self):
        """Test PROTO record type."""
        self.assertEqual(RecordType.PROTO.value, 1)
        self.assertEqual(int(RecordType.PROTO), 1)

    def test_json_constant(self):
        """Test JSON record type."""
        self.assertEqual(RecordType.JSON.value, 2)
        self.assertEqual(int(RecordType.JSON), 2)

    def test_record_type_equality(self):
        """Test RecordType equality."""
        proto1 = RecordType.PROTO
        proto2 = RecordType.PROTO
        json_type = RecordType.JSON

        self.assertEqual(proto1, proto2)
        self.assertNotEqual(proto1, json_type)


class TestApplicationName(unittest.TestCase):
    """Test the optional application_name SDK construction parameter.

    SDK construction validates the endpoint and extracts the workspace ID but
    does not open a network connection, so these tests run offline.
    """

    HOST = "https://test-workspace.zerobus.region.cloud.databricks.com"
    UC_URL = "https://test-workspace.cloud.databricks.com"

    def test_sync_sdk_constructs_with_application_name(self):
        """Sync SDK accepts application_name without error."""
        sdk = SyncSdk(self.HOST, self.UC_URL, application_name="my-app/1.0")
        self.assertIsNotNone(sdk)

    def test_async_sdk_constructs_with_application_name(self):
        """Async SDK accepts application_name without error."""
        sdk = AsyncSdk(self.HOST, self.UC_URL, application_name="my-app/1.0")
        self.assertIsNotNone(sdk)

    def test_sync_sdk_constructs_without_application_name(self):
        """application_name is optional; sync SDK still constructs by default."""
        sdk = SyncSdk(self.HOST, self.UC_URL)
        self.assertIsNotNone(sdk)

    def test_async_sdk_constructs_without_application_name(self):
        """application_name is optional; async SDK still constructs by default."""
        sdk = AsyncSdk(self.HOST, self.UC_URL)
        self.assertIsNotNone(sdk)

    def test_application_name_accepts_positional(self):
        """application_name may also be passed positionally."""
        self.assertIsNotNone(SyncSdk(self.HOST, self.UC_URL, "my-app/1.0"))
        self.assertIsNotNone(AsyncSdk(self.HOST, self.UC_URL, "my-app/1.0"))


class TestStreamConfigurationDefaults(unittest.TestCase):
    """Test StreamConfigurationOptions defaults."""

    def test_default_options(self):
        """Test that options can be created with defaults."""
        options = StreamConfigurationOptions()

        # Should have reasonable defaults
        self.assertIsNotNone(options.max_inflight_records)
        self.assertIsInstance(options.recovery, bool)
        self.assertIsNotNone(options.recovery_timeout_ms)

    def test_custom_options(self):
        """Test that options can be customized."""
        options = StreamConfigurationOptions(
            max_inflight_records=500,
            recovery=False,
            recovery_timeout_ms=5000,
            recovery_backoff_ms=1000,
            recovery_retries=5,
            record_type=RecordType.JSON,
        )

        self.assertEqual(options.max_inflight_records, 500)
        self.assertFalse(options.recovery)
        self.assertEqual(options.recovery_timeout_ms, 5000)
        self.assertEqual(options.recovery_backoff_ms, 1000)
        self.assertEqual(options.recovery_retries, 5)
        self.assertEqual(options.record_type, RecordType.JSON)


if __name__ == "__main__":
    unittest.main()
