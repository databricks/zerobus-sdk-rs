"""
Smoke tests for the Rust-backed Python SDK.

Verifies the v2.0.0 public API surface (imports, types, kwargs-only stream
creation) without making network connections. Integration testing is covered
by the Rust SDK's test suite and the LITE end-to-end suite.
"""

import unittest

from zerobus import (
    AckCallback,
    Auth,
    Format,
    Headers,
    HeadersProvider,
    NonRetriableException,
    OAuth,
    RecordType,
    StreamConfigurationOptions,
    ZerobusException,
    __version__,
)
from zerobus.sdk import ZerobusSdk as SyncSdk
from zerobus.sdk.aio import ZerobusSdk as AsyncSdk


class TestImports(unittest.TestCase):
    """Public-API symbols are importable."""

    def test_version_is_2(self):
        self.assertTrue(__version__.startswith("2."))

    def test_import_exceptions(self):
        self.assertTrue(issubclass(ZerobusException, Exception))
        self.assertTrue(issubclass(NonRetriableException, ZerobusException))

    def test_import_record_type(self):
        self.assertEqual(RecordType.PROTO.value, 1)
        self.assertEqual(RecordType.JSON.value, 2)
        self.assertEqual(int(RecordType.PROTO), 1)
        self.assertEqual(int(RecordType.JSON), 2)
        self.assertEqual(RecordType.PROTO, RecordType.PROTO)
        self.assertNotEqual(RecordType.PROTO, RecordType.JSON)

    def test_import_from_shared(self):
        from zerobus.sdk.shared import Format as SharedFormat
        from zerobus.sdk.shared import Headers as SharedHeaders
        from zerobus.sdk.shared import (
            HeadersProvider,
        )
        from zerobus.sdk.shared import OAuth as SharedOAuth
        from zerobus.sdk.shared import RecordType as SharedRecordType
        from zerobus.sdk.shared import (
            StreamConfigurationOptions,
        )

        self.assertEqual(SharedRecordType.PROTO.value, 1)
        self.assertIsNotNone(StreamConfigurationOptions)
        self.assertIsNotNone(HeadersProvider)
        self.assertIs(SharedFormat.JSON, Format.JSON)
        self.assertIs(SharedOAuth, OAuth)
        self.assertIs(SharedHeaders, Headers)

    def test_import_from_headers_provider_module(self):
        from zerobus.sdk.shared.headers_provider import HeadersProvider

        self.assertIsNotNone(HeadersProvider)

    def test_configuration_kwargs(self):
        options = StreamConfigurationOptions(
            max_inflight_records=100,
            recovery=True,
            record_type=RecordType.JSON,
        )
        self.assertEqual(options.max_inflight_records, 100)
        self.assertTrue(options.recovery)
        self.assertEqual(options.record_type, RecordType.JSON)

    def test_ackcallback_subclassable(self):
        class TestCallback(AckCallback):
            def on_ack(self, offset: int):
                pass

            def on_error(self, offset: int, error_message: str):
                pass

        self.assertIsInstance(TestCallback(), AckCallback)

    def test_headersprovider_subclassable(self):
        class CustomProvider(HeadersProvider):
            def get_headers(self):
                return [("authorization", "Bearer test")]

        provider = CustomProvider()
        self.assertIsInstance(provider, HeadersProvider)
        self.assertEqual(provider.get_headers(), [("authorization", "Bearer test")])


class TestSDKAPISurface(unittest.TestCase):
    """SDK classes expose the v2.0.0 method shape."""

    def test_sync_sdk_methods(self):
        for name in ("__init__", "create_stream", "recreate_stream", "create_arrow_stream", "recreate_arrow_stream"):
            self.assertTrue(hasattr(SyncSdk, name), f"SyncSdk missing {name}")

    def test_async_sdk_methods(self):
        import inspect

        for name in ("__init__", "create_stream", "recreate_stream", "create_arrow_stream", "recreate_arrow_stream"):
            self.assertTrue(hasattr(AsyncSdk, name), f"AsyncSdk missing {name}")
        # The async methods must actually be coroutines.
        for name in ("create_stream", "recreate_stream", "create_arrow_stream", "recreate_arrow_stream"):
            self.assertTrue(
                inspect.iscoroutinefunction(getattr(AsyncSdk, name)),
                f"AsyncSdk.{name} should be async",
            )

    def test_removed_v1_api_is_gone(self):
        """Symbols removed in v2.0.0 must not be importable."""
        # TableProperties / RecordAcknowledgment are gone.
        import zerobus

        self.assertFalse(hasattr(zerobus, "TableProperties"))
        self.assertFalse(hasattr(zerobus, "RecordAcknowledgment"))

        # Old per-auth-strategy methods are gone.
        self.assertFalse(hasattr(SyncSdk, "create_stream_with_headers_provider"))
        self.assertFalse(hasattr(SyncSdk, "create_arrow_stream_with_headers_provider"))
        self.assertFalse(hasattr(AsyncSdk, "create_stream_with_headers_provider"))
        self.assertFalse(hasattr(AsyncSdk, "create_arrow_stream_with_headers_provider"))

        # ingest_record() (deprecated since v0.3.0) is gone from both wrappers.
        from zerobus import _zerobus_core

        self.assertFalse(hasattr(_zerobus_core.sync.ZerobusStream, "ingest_record"))
        self.assertFalse(hasattr(_zerobus_core.aio.ZerobusStream, "ingest_record"))


class TestStreamAPISurface(unittest.TestCase):
    """Stream classes expose the expected methods."""

    def test_stream_methods_exist(self):
        from zerobus import _zerobus_core

        expected = [
            "ingest_record_offset",
            "ingest_record_nowait",
            "ingest_records_offset",
            "ingest_records_nowait",
            "wait_for_offset",
            "flush",
            "close",
            "get_unacked_records",
            "get_unacked_batches",
        ]
        for cls in (_zerobus_core.sync.ZerobusStream, _zerobus_core.aio.ZerobusStream):
            for m in expected:
                self.assertTrue(hasattr(cls, m), f"{cls.__name__} missing {m}")


class TestSelectorTypes(unittest.TestCase):
    """`OAuth`, `Headers`, `Format` selectors behave as documented."""

    def test_oauth_holds_fields(self):
        a = OAuth("id-1", "secret-1")
        self.assertEqual(a.client_id, "id-1")
        self.assertEqual(a.client_secret, "secret-1")

    def test_oauth_repr_hides_secret(self):
        """OAuth.client_secret must not appear in __repr__ (#11)."""
        a = OAuth("id-1", "VERY-SECRET")
        self.assertNotIn("VERY-SECRET", repr(a))
        self.assertIn("id-1", repr(a))

    def test_oauth_frozen(self):
        a = OAuth("id-1", "secret-1")
        with self.assertRaises(Exception):
            a.client_id = "id-2"  # type: ignore[misc]

    def test_headers_requires_subclass(self):
        with self.assertRaises(TypeError):
            Headers(provider="not-a-provider")  # type: ignore[arg-type]

    def test_headers_accepts_provider_subclass(self):
        class P(HeadersProvider):
            def get_headers(self):
                return []

        h = Headers(P())
        self.assertIsInstance(h.provider, HeadersProvider)

    def test_format_json_singleton(self):
        self.assertIs(Format.JSON, Format.JSON)

    def test_format_proto_rejects_none(self):
        with self.assertRaises(ValueError):
            Format.proto(None)

    def test_format_proto_extracts_message_name_from_descriptor(self):
        import os
        import sys

        sys.path.insert(0, os.path.dirname(__file__))
        import test_row_pb2  # noqa: E402

        f = Format.proto(test_row_pb2.AirQuality.DESCRIPTOR)
        self.assertEqual(f.message_name, "AirQuality")

    def test_format_proto_accepts_raw_bytes_without_name(self):
        # Raw bytes input cannot carry a name hint.
        import os
        import sys

        sys.path.insert(0, os.path.dirname(__file__))
        import test_row_pb2  # noqa: E402

        raw = test_row_pb2.AirQuality.DESCRIPTOR.file.serialized_pb
        f = Format.proto(raw)
        self.assertIsNone(f.message_name)

    def test_auth_union(self):
        # `Auth` is just `Union[OAuth, Headers]`; both should satisfy it.
        class P(HeadersProvider):
            def get_headers(self):
                return []

        for value in (OAuth("a", "b"), Headers(P())):
            self.assertIsInstance(value, (OAuth, Headers))


class TestSplitHelpers(unittest.TestCase):
    """Internal selector → binding-kwargs helpers route correctly."""

    def test_split_auth_oauth(self):
        from zerobus.sdk.sync.zerobus_sdk import _split_auth

        cid, secret, provider = _split_auth(OAuth("id", "secret"))
        self.assertEqual(cid, "id")
        self.assertEqual(secret, "secret")
        self.assertIsNone(provider)

    def test_split_auth_headers(self):
        from zerobus.sdk.sync.zerobus_sdk import _split_auth

        class P(HeadersProvider):
            def get_headers(self):
                return []

        provider_obj = P()
        cid, secret, provider = _split_auth(Headers(provider_obj))
        self.assertIsNone(cid)
        self.assertIsNone(secret)
        self.assertIs(provider, provider_obj)

    def test_split_auth_rejects_bare_dict(self):
        from zerobus.sdk.sync.zerobus_sdk import _split_auth

        with self.assertRaises(TypeError):
            _split_auth({"client_id": "x"})  # type: ignore[arg-type]

    def test_split_format_json(self):
        from zerobus.sdk.sync.zerobus_sdk import _split_format

        desc_bytes, name, rt = _split_format(Format.JSON)
        self.assertIsNone(desc_bytes)
        self.assertIsNone(name)
        self.assertEqual(rt, RecordType.JSON)

    def test_split_format_proto_with_descriptor(self):
        import os
        import sys

        from zerobus.sdk.sync.zerobus_sdk import _split_format

        sys.path.insert(0, os.path.dirname(__file__))
        import test_row_pb2  # noqa: E402

        desc_bytes, name, rt = _split_format(Format.proto(test_row_pb2.AirQuality.DESCRIPTOR))
        self.assertIsInstance(desc_bytes, bytes)
        self.assertGreater(len(desc_bytes), 0)
        self.assertEqual(name, "AirQuality")
        self.assertEqual(rt, RecordType.PROTO)

    def test_split_format_proto_with_raw_bytes_has_no_name(self):
        import os
        import sys

        from zerobus.sdk.sync.zerobus_sdk import _split_format

        sys.path.insert(0, os.path.dirname(__file__))
        import test_row_pb2  # noqa: E402

        raw = test_row_pb2.AirQuality.DESCRIPTOR.file.serialized_pb
        desc_bytes, name, rt = _split_format(Format.proto(raw))
        self.assertEqual(desc_bytes, raw)
        self.assertIsNone(name)
        self.assertEqual(rt, RecordType.PROTO)


class TestOptionsAreNotMutated(unittest.TestCase):
    """`_options_for_record_type` must not mutate the caller's options (#9)."""

    def test_options_clone_does_not_mutate_caller(self):
        from zerobus.sdk.sync.zerobus_sdk import _options_for_record_type

        original = StreamConfigurationOptions(record_type=RecordType.JSON)
        cloned = _options_for_record_type(original, RecordType.PROTO)

        # Caller's instance unchanged.
        self.assertEqual(original.record_type, RecordType.JSON)
        # Clone has the new record_type.
        self.assertEqual(cloned.record_type, RecordType.PROTO)
        # Distinct objects.
        self.assertIsNot(original, cloned)

    def test_options_none_yields_fresh(self):
        from zerobus.sdk.sync.zerobus_sdk import _options_for_record_type

        result = _options_for_record_type(None, RecordType.PROTO)
        self.assertEqual(result.record_type, RecordType.PROTO)

    def test_options_async_clone(self):
        """The async facade has its own helper; verify it behaves the same."""
        from zerobus.sdk.aio.zerobus_sdk import _options_for_record_type as aio_helper

        original = StreamConfigurationOptions(record_type=RecordType.JSON)
        cloned = aio_helper(original, RecordType.PROTO)
        self.assertEqual(original.record_type, RecordType.JSON)
        self.assertEqual(cloned.record_type, RecordType.PROTO)


class TestStreamConfigurationDefaults(unittest.TestCase):
    """`StreamConfigurationOptions` defaults match the documented values."""

    def test_default_options(self):
        options = StreamConfigurationOptions()
        self.assertEqual(options.max_inflight_records, 50_000)
        self.assertTrue(options.recovery)
        self.assertEqual(options.recovery_timeout_ms, 15_000)
        self.assertEqual(options.recovery_backoff_ms, 2_000)
        self.assertEqual(options.recovery_retries, 3)
        self.assertEqual(options.flush_timeout_ms, 300_000)
        self.assertEqual(options.server_lack_of_ack_timeout_ms, 60_000)
        self.assertEqual(options.record_type, RecordType.PROTO)
        self.assertIsNone(options.stream_paused_max_wait_time_ms)
        self.assertEqual(options.callback_max_wait_time_ms, 5_000)
        self.assertIsNone(options.ack_callback)

    def test_custom_options(self):
        options = StreamConfigurationOptions(
            max_inflight_records=500,
            recovery=False,
            recovery_timeout_ms=5_000,
            recovery_backoff_ms=1_000,
            recovery_retries=5,
            record_type=RecordType.JSON,
            stream_paused_max_wait_time_ms=10_000,
        )
        self.assertEqual(options.max_inflight_records, 500)
        self.assertFalse(options.recovery)
        self.assertEqual(options.recovery_timeout_ms, 5_000)
        self.assertEqual(options.recovery_backoff_ms, 1_000)
        self.assertEqual(options.recovery_retries, 5)
        self.assertEqual(options.record_type, RecordType.JSON)
        self.assertEqual(options.stream_paused_max_wait_time_ms, 10_000)


class TestAuthAlias(unittest.TestCase):
    """`Auth` is a `Union[OAuth, Headers]` alias used for typing."""

    def test_auth_alias_present(self):
        from typing import Union, get_args, get_origin

        # get_origin on a Union returns Union itself.
        self.assertEqual(get_origin(Auth), get_origin(Union[OAuth, Headers]))
        self.assertEqual(set(get_args(Auth)), {OAuth, Headers})


if __name__ == "__main__":
    unittest.main()
