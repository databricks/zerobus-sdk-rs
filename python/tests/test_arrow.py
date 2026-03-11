"""
Tests for Arrow Flight support in the Python SDK.

These tests verify the Python-side Arrow serialization/deserialization helpers,
the ArrowStreamConfigurationOptions pyclass, and the API surface of Arrow stream
classe, without making network connections.
"""

import unittest

import pyarrow as pa

from zerobus.sdk.shared.arrow import (
    ArrowStreamConfigurationOptions,
    _check_pyarrow,
    _deserialize_batch,
    _serialize_batch,
    _serialize_schema,
)


class TestCheckPyarrow(unittest.TestCase):
    """Test the pyarrow availability check."""

    def test_check_pyarrow_returns_module(self):
        result = _check_pyarrow()
        self.assertIs(result, pa)


class TestSerializeSchema(unittest.TestCase):
    """Test schema serialization to IPC bytes."""

    def test_simple_schema(self):
        schema = pa.schema([("a", pa.int64()), ("b", pa.utf8())])
        ipc_bytes = _serialize_schema(schema)
        self.assertIsInstance(ipc_bytes, bytes)
        self.assertGreater(len(ipc_bytes), 0)

    def test_schema_with_various_types(self):
        schema = pa.schema([
            ("int_col", pa.int32()),
            ("float_col", pa.float64()),
            ("str_col", pa.large_utf8()),
            ("bool_col", pa.bool_()),
            ("list_col", pa.list_(pa.int64())),
        ])
        ipc_bytes = _serialize_schema(schema)
        self.assertIsInstance(ipc_bytes, bytes)

    def test_roundtrip_schema_via_ipc(self):
        schema = pa.schema([("x", pa.int64()), ("y", pa.float32())])
        ipc_bytes = _serialize_schema(schema)
        reader = pa.ipc.open_stream(ipc_bytes)
        recovered = reader.schema
        self.assertEqual(schema, recovered)

    def test_rejects_non_schema(self):
        with self.assertRaises(TypeError):
            _serialize_schema("not a schema")

    def test_rejects_record_batch(self):
        batch = pa.record_batch({"a": [1]}, schema=pa.schema([("a", pa.int64())]))
        with self.assertRaises(TypeError):
            _serialize_schema(batch)


class TestSerializeBatch(unittest.TestCase):
    """Test RecordBatch/Table serialization to IPC bytes."""

    def test_simple_batch(self):
        schema = pa.schema([("a", pa.int64())])
        batch = pa.record_batch({"a": [1, 2, 3]}, schema=schema)
        ipc_bytes = _serialize_batch(batch)
        self.assertIsInstance(ipc_bytes, bytes)
        self.assertGreater(len(ipc_bytes), 0)

    def test_batch_with_multiple_columns(self):
        schema = pa.schema([("x", pa.int32()), ("y", pa.utf8()), ("z", pa.float64())])
        batch = pa.record_batch(
            {"x": [1, 2], "y": ["a", "b"], "z": [1.0, 2.0]},
            schema=schema,
        )
        ipc_bytes = _serialize_batch(batch)
        self.assertIsInstance(ipc_bytes, bytes)

    def test_table_single_chunk(self):
        schema = pa.schema([("a", pa.int64())])
        table = pa.table({"a": [1, 2, 3]}, schema=schema)
        ipc_bytes = _serialize_batch(table)
        self.assertIsInstance(ipc_bytes, bytes)

    def test_table_multiple_chunks(self):
        schema = pa.schema([("a", pa.int64())])
        batch1 = pa.record_batch({"a": [1, 2]}, schema=schema)
        batch2 = pa.record_batch({"a": [3, 4]}, schema=schema)
        table = pa.Table.from_batches([batch1, batch2])
        ipc_bytes = _serialize_batch(table)
        # Should combine chunks into a single batch
        recovered = _deserialize_batch(ipc_bytes)
        self.assertEqual(recovered.num_rows, 4)

    def test_empty_table_raises(self):
        schema = pa.schema([("a", pa.int64())])
        table = pa.table({"a": pa.array([], type=pa.int64())}, schema=schema)
        with self.assertRaises(ValueError):
            _serialize_batch(table)

    def test_rejects_wrong_type(self):
        with self.assertRaises(TypeError):
            _serialize_batch("not a batch")

    def test_rejects_dict(self):
        with self.assertRaises(TypeError):
            _serialize_batch({"a": [1, 2, 3]})

    def test_rejects_schema(self):
        with self.assertRaises(TypeError):
            _serialize_batch(pa.schema([("a", pa.int64())]))


class TestDeserializeBatch(unittest.TestCase):
    """Test IPC bytes deserialization back to RecordBatch."""

    def test_roundtrip(self):
        schema = pa.schema([("a", pa.int64()), ("b", pa.utf8())])
        original = pa.record_batch(
            {"a": [1, 2, 3], "b": ["x", "y", "z"]},
            schema=schema,
        )
        ipc_bytes = _serialize_batch(original)
        recovered = _deserialize_batch(ipc_bytes)

        self.assertIsInstance(recovered, pa.RecordBatch)
        self.assertEqual(recovered.schema, original.schema)
        self.assertEqual(recovered.num_rows, original.num_rows)
        self.assertEqual(recovered.to_pydict(), original.to_pydict())

    def test_roundtrip_with_nulls(self):
        schema = pa.schema([("a", pa.int64()), ("b", pa.utf8())])
        original = pa.record_batch(
            {"a": [1, None, 3], "b": [None, "y", None]},
            schema=schema,
        )
        ipc_bytes = _serialize_batch(original)
        recovered = _deserialize_batch(ipc_bytes)

        self.assertEqual(recovered.to_pydict(), original.to_pydict())

    def test_roundtrip_table(self):
        schema = pa.schema([("val", pa.float64())])
        table = pa.table({"val": [1.1, 2.2, 3.3]}, schema=schema)
        ipc_bytes = _serialize_batch(table)
        recovered = _deserialize_batch(ipc_bytes)

        self.assertIsInstance(recovered, pa.RecordBatch)
        self.assertEqual(recovered.num_rows, 3)

    def test_invalid_bytes_raises(self):
        with self.assertRaises(Exception):
            _deserialize_batch(b"not valid ipc data")


class TestArrowStreamConfigurationOptions(unittest.TestCase):
    """Test ArrowStreamConfigurationOptions pyclass."""

    def test_default_construction(self):
        options = ArrowStreamConfigurationOptions()
        self.assertIsInstance(options.max_inflight_batches, int)
        self.assertIsInstance(options.recovery, bool)
        self.assertIsInstance(options.recovery_timeout_ms, int)
        self.assertIsInstance(options.recovery_backoff_ms, int)
        self.assertIsInstance(options.recovery_retries, int)
        self.assertIsInstance(options.server_lack_of_ack_timeout_ms, int)
        self.assertIsInstance(options.flush_timeout_ms, int)
        self.assertIsInstance(options.connection_timeout_ms, int)

    def test_kwargs_construction(self):
        options = ArrowStreamConfigurationOptions(
            max_inflight_batches=5,
            recovery=False,
            flush_timeout_ms=3000,
        )
        self.assertEqual(options.max_inflight_batches, 5)
        self.assertFalse(options.recovery)
        self.assertEqual(options.flush_timeout_ms, 3000)

    def test_all_kwargs(self):
        options = ArrowStreamConfigurationOptions(
            max_inflight_batches=20,
            recovery=True,
            recovery_timeout_ms=10000,
            recovery_backoff_ms=500,
            recovery_retries=10,
            server_lack_of_ack_timeout_ms=30000,
            flush_timeout_ms=5000,
            connection_timeout_ms=8000,
        )
        self.assertEqual(options.max_inflight_batches, 20)
        self.assertTrue(options.recovery)
        self.assertEqual(options.recovery_timeout_ms, 10000)
        self.assertEqual(options.recovery_backoff_ms, 500)
        self.assertEqual(options.recovery_retries, 10)
        self.assertEqual(options.server_lack_of_ack_timeout_ms, 30000)
        self.assertEqual(options.flush_timeout_ms, 5000)
        self.assertEqual(options.connection_timeout_ms, 8000)

    def test_unknown_kwarg_raises(self):
        with self.assertRaises(ValueError):
            ArrowStreamConfigurationOptions(nonexistent_option=42)

    def test_setters(self):
        options = ArrowStreamConfigurationOptions()
        options.max_inflight_batches = 99
        options.recovery = False
        self.assertEqual(options.max_inflight_batches, 99)
        self.assertFalse(options.recovery)

    def test_repr(self):
        options = ArrowStreamConfigurationOptions()
        repr_str = repr(options)
        self.assertIn("ArrowStreamConfigurationOptions", repr_str)
        self.assertIn("max_inflight_batches", repr_str)
        self.assertIn("recovery", repr_str)


class TestArrowImports(unittest.TestCase):
    """Test that Arrow types can be imported from expected locations."""

    def test_import_from_shared_arrow(self):
        from zerobus.sdk.shared.arrow import ArrowStreamConfigurationOptions
        self.assertIsNotNone(ArrowStreamConfigurationOptions)

    def test_import_from_top_level(self):
        from zerobus import ArrowStreamConfigurationOptions
        self.assertIsNotNone(ArrowStreamConfigurationOptions)

    def test_import_arrow_stream_from_sync(self):
        from zerobus.sdk.sync import ZerobusArrowStream
        self.assertIsNotNone(ZerobusArrowStream)

    def test_import_arrow_stream_from_aio(self):
        from zerobus.sdk.aio import ZerobusArrowStream
        self.assertIsNotNone(ZerobusArrowStream)

    def test_import_arrow_stream_from_top_level(self):
        from zerobus import ZerobusArrowStream
        self.assertIsNotNone(ZerobusArrowStream)


class TestArrowSDKAPISurface(unittest.TestCase):
    """Test that Arrow SDK classes have the expected API surface."""

    def test_sync_sdk_has_arrow_methods(self):
        from zerobus.sdk.sync import ZerobusSdk
        self.assertTrue(hasattr(ZerobusSdk, "create_arrow_stream"))
        self.assertTrue(hasattr(ZerobusSdk, "recreate_arrow_stream"))

    def test_async_sdk_has_arrow_methods(self):
        from zerobus.sdk.aio import ZerobusSdk
        self.assertTrue(hasattr(ZerobusSdk, "create_arrow_stream"))
        self.assertTrue(hasattr(ZerobusSdk, "recreate_arrow_stream"))

    def test_sync_arrow_stream_has_methods(self):
        from zerobus.sdk.sync import ZerobusArrowStream
        expected_methods = [
            "ingest_batch",
            "wait_for_offset",
            "flush",
            "close",
            "get_unacked_batches",
        ]
        for method in expected_methods:
            self.assertTrue(
                hasattr(ZerobusArrowStream, method),
                f"Sync ZerobusArrowStream missing method: {method}",
            )

    def test_async_arrow_stream_has_methods(self):
        from zerobus.sdk.aio import ZerobusArrowStream
        expected_methods = [
            "ingest_batch",
            "wait_for_offset",
            "flush",
            "close",
            "get_unacked_batches",
        ]
        for method in expected_methods:
            self.assertTrue(
                hasattr(ZerobusArrowStream, method),
                f"Async ZerobusArrowStream missing method: {method}",
            )

    def test_arrow_types_not_on_core_top_level(self):
        """Arrow types should be in _core.arrow submodule, not on _core directly."""
        import zerobus._zerobus_core as _core
        self.assertFalse(hasattr(_core, "ArrowStreamConfigurationOptions"))
        self.assertFalse(hasattr(_core, "ZerobusArrowStream"))
        self.assertFalse(hasattr(_core, "AsyncZerobusArrowStream"))

    def test_arrow_types_in_core_arrow_submodule(self):
        """Arrow types should be accessible via _core.arrow."""
        import zerobus._zerobus_core as _core
        self.assertTrue(hasattr(_core.arrow, "ArrowStreamConfigurationOptions"))
        self.assertTrue(hasattr(_core.arrow, "ZerobusArrowStream"))
        self.assertTrue(hasattr(_core.arrow, "AsyncZerobusArrowStream"))


if __name__ == "__main__":
    unittest.main()
