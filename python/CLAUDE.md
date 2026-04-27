# Python SDK

Python wrapper around the Rust core via PyO3 and maturin.

## Structure

```
python/
├── rust/src/lib.rs   # PyO3 bindings (the Rust ↔ Python bridge)
├── zerobus/          # Python package (pure-Python layer over _zerobus_core)
├── tests/            # pytest test suite
└── pyproject.toml    # Build config (maturin)
```

The native extension module is `_zerobus_core`, exposed as three submodules:
- `zerobus._zerobus_core.sync` — Synchronous API
- `zerobus._zerobus_core.aio` — Async API (Python `async/await`)
- `zerobus._zerobus_core.arrow` — Arrow Flight support

## Build commands

Run from `python/`:

- `make dev` — Create venv, install deps, build Rust extension
- `make build-rust` — Release build of the Rust extension
- `make develop-rust` — Debug build (faster iteration)
- `make test` — pytest with coverage
- `make lint` — pycodestyle + autoflake
- `make fmt` — black + autoflake + isort

## FFI boundary: PyO3

PyO3 handles memory management automatically via Python reference counting. Key considerations:

- **GIL release**: Async operations release the GIL, allowing concurrent Python threads. The tokio runtime is initialized at module import via `pyo3_asyncio::tokio::init()`.
- **Object lifetime**: PyO3 classes are ref-counted by Python's GC. No explicit free needed, but `close()` should still be called to flush pending records before GC cleanup.
- **Error mapping**: Rust `ZerobusError` maps to `ZerobusException` / `NonRetriableException` Python exception classes.
- **Serialization**: Proto descriptors and record payloads cross as Python `bytes`. JSON records cross as Python `str`. No extra serialization layer — PyO3 handles the conversion.

## Breaking change rules

The public Python API is defined by the classes and functions in `zerobus/`. Changes here follow semver:

- Removing a class, method, or parameter is breaking.
- Deprecate with `warnings.warn(..., DeprecationWarning)` first.
- The PyO3 binding layer (`python/rust/src/lib.rs`) is internal; refactoring it is safe as long as the Python-facing API doesn't change.

## Performance notes

- PyO3 has minimal overhead for `bytes`/`str` payloads (no copy for immutable buffers).
- Async API avoids GIL contention — prefer it for high-throughput ingestion.
- Batch ingestion (`ingest_records`) amortizes the Python→Rust call overhead.

## Changelog and documentation

- Every PR must update `python/NEXT_CHANGELOG.md` under the appropriate section if it changes user-facing behavior.
- Update `python/README.md` if the change affects usage, setup, or API surface.
- Add or update examples in `python/examples/` for new or modified APIs.
- Add docstrings for all new public classes/methods. Update type stubs (`_zerobus_core.pyi`) if the native module interface changes.

## Release

- Version is derived from `python/rust/Cargo.toml` via maturin (no separate Python version file).
- Tag: `python/v<semver>` → triggers `release-python.yml` → builds wheels for all platforms/Python versions → publishes to PyPI.
- On version bump PR: update the version in `python/rust/Cargo.toml`, move `NEXT_CHANGELOG.md` contents to `CHANGELOG.md`, reset `NEXT_CHANGELOG.md`.

## Config

- Python >= 3.9 required
- Line length: 120 (black)
- Formatter: black, isort
