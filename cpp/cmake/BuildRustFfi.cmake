# Builds the Zerobus C FFI static library from the local Rust source and exposes
# it as an imported target `zerobus::ffi`.
#
# Override the prebuilt library and header instead of building from source by
# setting -DZEROBUS_FFI_LIBRARY=<path-to-.a> and -DZEROBUS_FFI_HEADER_DIR=<dir>.

set(ZEROBUS_REPO_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/.." CACHE PATH
    "Path to the zerobus-sdk monorepo root")
set(ZEROBUS_FFI_CRATE_DIR "${ZEROBUS_REPO_ROOT}/rust/ffi")
set(ZEROBUS_RUST_TARGET_DIR "${ZEROBUS_REPO_ROOT}/rust/target")

if(WIN32)
  set(_zb_ffi_lib_name "zerobus_ffi.lib")
else()
  set(_zb_ffi_lib_name "libzerobus_ffi.a")
endif()

add_library(zerobus_ffi STATIC IMPORTED GLOBAL)
add_library(zerobus::ffi ALIAS zerobus_ffi)

if(DEFINED ZEROBUS_FFI_LIBRARY)
  # Use a prebuilt/vendored static library.
  if(NOT DEFINED ZEROBUS_FFI_HEADER_DIR)
    set(ZEROBUS_FFI_HEADER_DIR "${ZEROBUS_FFI_CRATE_DIR}")
  endif()
  set_target_properties(zerobus_ffi PROPERTIES
      IMPORTED_LOCATION "${ZEROBUS_FFI_LIBRARY}"
      INTERFACE_INCLUDE_DIRECTORIES "${ZEROBUS_FFI_HEADER_DIR}")
  message(STATUS "Zerobus: using prebuilt FFI library ${ZEROBUS_FFI_LIBRARY}")
else()
  # Build from local Rust source via cargo.
  find_program(CARGO_EXECUTABLE cargo REQUIRED
      DOC "Path to the cargo build tool")

  # When a sanitizer is requested, instrument the Rust core too — otherwise TSan
  # / ASan only watch the thin C++ wrapper and miss races/UAF inside the FFI,
  # where the real work runs. -Zsanitizer + -Zbuild-std are nightly-only and
  # need a --target (which relocates the archive under target/<triple>/). ASan
  # and TSan map to the sanitizer runtime; other values (e.g. undefined) fall
  # back to a plain release build.
  set(_zb_cargo "${CARGO_EXECUTABLE}")
  set(_zb_cargo_flags "")
  set(_zb_target_dir "${ZEROBUS_RUST_TARGET_DIR}")
  set(_zb_ffi_lib "${ZEROBUS_RUST_TARGET_DIR}/release/${_zb_ffi_lib_name}")
  if(ZEROBUS_SANITIZE STREQUAL "thread" OR ZEROBUS_SANITIZE STREQUAL "address")
    execute_process(COMMAND "${CARGO_EXECUTABLE}" -vV
        OUTPUT_VARIABLE _zb_rustc_v OUTPUT_STRIP_TRAILING_WHITESPACE)
    string(REGEX MATCH "host: ([^\n]+)" _zb_host_match "${_zb_rustc_v}")
    set(_zb_host "${CMAKE_MATCH_1}")
    # Per-sanitizer target dir: the --target output path is the same for asan and
    # tsan, and changing only RUSTFLAGS does not invalidate cargo's cache, so a
    # shared dir would link a stale archive built for the other sanitizer.
    set(_zb_target_dir "${ZEROBUS_RUST_TARGET_DIR}/sanitize-${ZEROBUS_SANITIZE}")
    set(_zb_cargo "${CARGO_EXECUTABLE}" "+nightly")
    set(_zb_cargo_flags -Z build-std --target "${_zb_host}"
        --target-dir "${_zb_target_dir}")
    set(_zb_ffi_lib
        "${_zb_target_dir}/${_zb_host}/release/${_zb_ffi_lib_name}")
    message(STATUS
        "Zerobus: building FFI with -Zsanitizer=${ZEROBUS_SANITIZE} (nightly, build-std)")
  endif()

  # Track the Rust sources the archive is built from so editing rust/ffi or
  # rust/sdk re-invokes cargo. Without DEPENDS, CMake treats the archive as
  # up-to-date once it exists and links stale Rust. CONFIGURE_DEPENDS re-globs
  # at build time so newly added .rs files are picked up too. (cargo itself is
  # incremental, so the rebuild is cheap when nothing changed.)
  file(GLOB_RECURSE _zb_ffi_rust_sources CONFIGURE_DEPENDS
      "${ZEROBUS_FFI_CRATE_DIR}/src/*.rs"
      "${ZEROBUS_REPO_ROOT}/rust/sdk/src/*.rs")
  list(APPEND _zb_ffi_rust_sources
      "${ZEROBUS_FFI_CRATE_DIR}/Cargo.toml"
      "${ZEROBUS_REPO_ROOT}/rust/sdk/Cargo.toml"
      "${ZEROBUS_REPO_ROOT}/rust/Cargo.toml")

  # Instrumented builds pass RUSTFLAGS via the environment so they don't leak
  # into unrelated cargo invocations.
  if(ZEROBUS_SANITIZE STREQUAL "thread" OR ZEROBUS_SANITIZE STREQUAL "address")
    set(_zb_build_cmd ${CMAKE_COMMAND} -E env
        "RUSTFLAGS=-Zsanitizer=${ZEROBUS_SANITIZE}"
        ${_zb_cargo} build ${_zb_cargo_flags} --release)
  else()
    set(_zb_build_cmd ${_zb_cargo} build --release)
  endif()

  add_custom_command(
      OUTPUT "${_zb_ffi_lib}"
      COMMAND ${_zb_build_cmd}
      DEPENDS ${_zb_ffi_rust_sources}
      WORKING_DIRECTORY "${ZEROBUS_FFI_CRATE_DIR}"
      COMMENT "Building Zerobus C FFI (cargo build --release)"
      VERBATIM)
  add_custom_target(zerobus_ffi_build DEPENDS "${_zb_ffi_lib}")

  set_target_properties(zerobus_ffi PROPERTIES
      IMPORTED_LOCATION "${_zb_ffi_lib}"
      INTERFACE_INCLUDE_DIRECTORIES "${ZEROBUS_FFI_CRATE_DIR}")
  # Consumers must build the cargo target first; the imported target itself
  # cannot carry the dependency, so callers add it to their own targets.
  set(ZEROBUS_FFI_BUILD_TARGET zerobus_ffi_build CACHE INTERNAL "")
endif()

# System libraries the Rust static library needs at link time. These mirror the
# CGO LDFLAGS in go/ffi.go.
if(APPLE)
  target_link_libraries(zerobus_ffi INTERFACE
      "-framework CoreFoundation" "-framework Security" iconv)
elseif(WIN32)
  target_link_libraries(zerobus_ffi INTERFACE
      ws2_32 userenv bcrypt ntdll)
else()  # Linux / other Unix
  find_package(Threads REQUIRED)
  target_link_libraries(zerobus_ffi INTERFACE
      Threads::Threads ${CMAKE_DL_LIBS} m resolv)
endif()
