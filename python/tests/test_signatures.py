"""Regression tests for native binding signatures (issue #726).

Optional arguments must keep their ``None`` default. PyO3 0.23+ dropped the
implicit default for ``Option<T>`` arguments, so it only survives with an
explicit ``#[pyo3(signature = ...)]`` attribute. These tests need only the
built extension (no network, no credentials).
"""

import inspect
import unittest

from zerobus import _zerobus_core as core


class TestNativeBindingSignatures(unittest.TestCase):
    def _assert_optional(self, fn, param_name):
        params = inspect.signature(fn).parameters
        self.assertIn(param_name, params)
        self.assertIs(
            params[param_name].default,
            None,
            f"{fn.__qualname__} parameter '{param_name}' must default to None, "
            f"got {params[param_name].default!r}",
        )

    def test_sync_wait_for_offset_timeout_optional(self):
        self._assert_optional(core.sync.ZerobusStream.wait_for_offset, "timeout_sec")

    def test_sync_wait_for_offset_accepts_offset_only(self):
        # The public wrapper calls self._inner.wait_for_offset(offset).
        sig = inspect.signature(core.sync.ZerobusStream.wait_for_offset)
        try:
            sig.bind(object(), 0)
        except TypeError as exc:
            self.fail(f"wait_for_offset(offset) is not bindable: {exc}")

    def test_sync_wait_for_ack_timeout_optional(self):
        self._assert_optional(
            core.sync.RecordAcknowledgment.wait_for_ack, "_timeout_sec"
        )

    def test_sync_create_stream_options_optional(self):
        self._assert_optional(core.sync.ZerobusSdk.create_stream, "options")

    def test_sync_create_stream_with_headers_provider_options_optional(self):
        self._assert_optional(
            core.sync.ZerobusSdk.create_stream_with_headers_provider, "options"
        )

    def test_async_create_stream_options_optional(self):
        self._assert_optional(core.aio.ZerobusSdk.create_stream, "options")

    def test_async_create_stream_with_headers_provider_options_optional(self):
        self._assert_optional(
            core.aio.ZerobusSdk.create_stream_with_headers_provider, "options"
        )

    def test_async_wait_for_offset_accepts_offset_only(self):
        sig = inspect.signature(core.aio.ZerobusStream.wait_for_offset)
        try:
            sig.bind(object(), 0)
        except TypeError as exc:
            self.fail(f"async wait_for_offset(offset) is not bindable: {exc}")


if __name__ == "__main__":
    unittest.main()
