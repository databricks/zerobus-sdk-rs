"""
Arrow Flight support for the Zerobus SDK.

**Experimental/Unsupported**: Arrow Flight support is experimental and not yet
supported for production use. The API may change in future releases.

Requires pyarrow to be installed:
    pip install databricks-zerobus-ingest-sdk[arrow]

Example (Sync):
    >>> import pyarrow as pa
    >>> from zerobus.sdk.sync import ZerobusSdk
    >>> from zerobus.sdk.shared.arrow import ArrowStreamConfigurationOptions
    >>>
    >>> sdk = ZerobusSdk(host, unity_catalog_url)
    >>> schema = pa.schema([("device_name", pa.large_utf8()), ("temp", pa.int32())])
    >>> stream = sdk.create_arrow_stream(
    ...     "catalog.schema.table", schema, client_id, client_secret
    ... )
    >>> batch = pa.record_batch({"device_name": ["s1"], "temp": [22]}, schema=schema)
    >>> offset = stream.ingest_batch(batch)
    >>> stream.wait_for_offset(offset)
    >>> stream.close()
"""

_PYARROW_IMPORT_ERROR = (
    "pyarrow is required for Arrow Flight support. "
    "Install with: pip install databricks-zerobus-ingest-sdk[arrow]"
)


def _check_pyarrow():
    """Check that pyarrow is available, raise ImportError if not."""
    try:
        import pyarrow  # noqa: F401

        return pyarrow
    except ImportError:
        raise ImportError(_PYARROW_IMPORT_ERROR)


def _serialize_schema(schema):
    """Serialize a pyarrow.Schema to IPC bytes."""
    pa = _check_pyarrow()
    if not isinstance(schema, pa.Schema):
        raise TypeError(f"Expected pyarrow.Schema, got {type(schema).__name__}")

    # Create an empty RecordBatch with the schema and serialize it.
    # This produces a valid IPC stream that the Rust side can parse for the schema.
    sink = pa.BufferOutputStream()
    writer = pa.ipc.new_stream(sink, schema)
    writer.close()
    return sink.getvalue().to_pybytes()


def _serialize_batch(batch):
    """Serialize a pyarrow.RecordBatch (or Table) to IPC bytes."""
    pa = _check_pyarrow()
    if isinstance(batch, pa.Table):
        # Convert Table to a single RecordBatch
        batches = batch.to_batches()
        if len(batches) == 0:
            raise ValueError("Cannot ingest an empty pyarrow.Table")
        if len(batches) > 1:
            batch = pa.concat_tables([pa.Table.from_batches([b]) for b in batches]).combine_chunks().to_batches()[0]
        else:
            batch = batches[0]
    elif not isinstance(batch, pa.RecordBatch):
        raise TypeError(
            f"Expected pyarrow.RecordBatch or pyarrow.Table, got {type(batch).__name__}"
        )

    sink = pa.BufferOutputStream()
    writer = pa.ipc.new_stream(sink, batch.schema)
    writer.write_batch(batch)
    writer.close()
    return sink.getvalue().to_pybytes()


def _deserialize_batch(ipc_bytes):
    """Deserialize IPC bytes back to a pyarrow.RecordBatch."""
    pa = _check_pyarrow()
    reader = pa.ipc.open_stream(ipc_bytes)
    return reader.read_next_batch()


# Re-export configuration from Rust core
import zerobus._zerobus_core as _core

ArrowStreamConfigurationOptions = _core.arrow.ArrowStreamConfigurationOptions
