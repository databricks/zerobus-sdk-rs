# NEXT CHANGELOG

## Release v1.5.0

### Major Changes

### New Features and Improvements

- Added support for CPython 3.13 and 3.14. Both versions now run in CI, on Linux
  and on Windows.
- The `arrow` extra now selects `pyarrow` by Python version. pyarrow dropped
  CPython 3.9 before it added CPython 3.14, so no single version covers the range
  this SDK supports: 21.0.0 is the last release with 3.9 wheels and 22.0.0 is the
  first with 3.14 wheels. The extra now requires `pyarrow < 22.0` below Python
  3.14 and `pyarrow >= 22.0` from 3.14 up. Arrow ingestion works on 3.14; the
  previous `< 20.0` ceiling excluded every version that supports it.

### Bug Fixes

- Fixed a segmentation fault on CPython 3.14. The SDK used PyO3 0.20, which
  supports CPython up to 3.12. The wheel targets the stable ABI (`abi3`), so pip
  installed it on 3.14 and the process crashed when you created a `ZerobusSdk`.
  The bindings now use PyO3 0.29, which supports 3.14.
- `requires-python` is now `>=3.9,<3.15`, so pip no longer installs the SDK on a
  CPython version it has not been tested against. The package declared no upper
  bound before, which is what let pip pick the `abi3` wheel on 3.14 and crash
  rather than report that the version is unsupported. The bound moves up as each
  new CPython version is added to CI.
- Updated the native extension's transitive `quinn-proto`, `rustls-webpki`, and
  `rand` dependencies to patched releases.

### Documentation

### Internal Changes

- Migrated the PyO3 bindings from 0.20 to 0.29. This replaces the removed
  GIL-reference API with the `Bound<'py, T>` API, renames `Python::with_gil` to
  `Python::attach` and `Python::allow_threads` to `Python::detach`, and replaces
  `downcast` with `cast`.
- Replaced the deprecated `pyo3-asyncio` crate with `pyo3-async-runtimes` 0.29.
- `StreamConfigurationOptions` now implements `Clone` by hand. `Py<T>` is no
  longer unconditionally `Clone` in PyO3 0.29, and the hand-written impl attaches
  the interpreter to copy the `ack_callback` handle. This avoids PyO3's `py-clone`
  feature, which panics when the interpreter is detached — the exact state of the
  async stream-builder paths.
- The wrapper crate now declares `rust-version = "1.88"`, matching the effective
  requirement from Tonic 0.14.6. This does not change the Rust core SDK's own MSRV.
- The Arrow test module now skips itself when `pyarrow` is absent. `pyarrow` is an
  optional dependency, but the module imported it at the top level, so the whole
  test suite failed to collect on an install without the `arrow` extra.
- Added Dependabot coverage for the Python extension's Rust crate under `python/rust`.

### Breaking Changes

### Deprecations

### API Changes
