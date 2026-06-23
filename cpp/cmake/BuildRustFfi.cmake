# Builds the Zerobus C FFI static library from the local Rust source and exposes
# it as an imported target `zerobus::ffi`.
#
# Mirrors go/build_rust.sh: invokes `cargo build --release` in `rust/ffi`, which
# produces `rust/target/release/libzerobus_ffi.a` and regenerates `zerobus.h`
# (via the crate's cbindgen build script). The header directory is added to the
# imported target's interface includes.
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

  set(_zb_ffi_lib "${ZEROBUS_RUST_TARGET_DIR}/release/${_zb_ffi_lib_name}")

  add_custom_command(
      OUTPUT "${_zb_ffi_lib}"
      COMMAND "${CARGO_EXECUTABLE}" build --release
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
