cmake_minimum_required(VERSION 3.16)

project(zerobus_cpp
    VERSION 0.1.0
    DESCRIPTION "Zerobus C++ SDK (wrapper over the Zerobus C FFI)"
    LANGUAGES CXX)

# Detect whether we are the top-level project (controls test/example defaults).
if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
  set(ZEROBUS_IS_TOP_LEVEL ON)
else()
  set(ZEROBUS_IS_TOP_LEVEL OFF)
endif()

option(ZEROBUS_BUILD_TESTS "Build the Zerobus C++ SDK tests" ${ZEROBUS_IS_TOP_LEVEL})
option(ZEROBUS_BUILD_EXAMPLES "Build the Zerobus C++ SDK examples" ${ZEROBUS_IS_TOP_LEVEL})

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

# ---------------------------------------------------------------------------
# Sanitizers (off by default)
# ---------------------------------------------------------------------------
# Build the SDK, tests, and examples with a runtime sanitizer so the existing
# test suite also catches the memory/threading bugs a thin FFI wrapper is prone
# to (use-after-free, double-free, leaks, data races). No extra dependencies —
# the sanitizers ship with GCC/Clang. Enable with e.g. -DZEROBUS_SANITIZE=address
# (others: thread, undefined). The flags must apply to compiling AND linking and
# to every target, so they are set globally here, before any target is defined.
set(ZEROBUS_SANITIZE "" CACHE STRING
    "Runtime sanitizer to build with: address, thread, undefined (empty = none)")

if(ZEROBUS_SANITIZE)
  if(NOT (CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang"))
    message(FATAL_ERROR
        "ZEROBUS_SANITIZE requires GCC or Clang (got ${CMAKE_CXX_COMPILER_ID})")
  endif()
  message(STATUS "Zerobus: building with -fsanitize=${ZEROBUS_SANITIZE}")
  # -fno-omit-frame-pointer + -g give readable stack traces in reports.
  add_compile_options(-fsanitize=${ZEROBUS_SANITIZE} -fno-omit-frame-pointer -g)
  add_link_options(-fsanitize=${ZEROBUS_SANITIZE})
endif()

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
include(BuildRustFfi)

# ---------------------------------------------------------------------------
# Library target
# ---------------------------------------------------------------------------
add_library(zerobus_cpp
    src/sdk.cpp
    src/stream.cpp
    src/arrow_stream.cpp
    src/proto_schema.cpp
    src/headers_callback.cpp)
add_library(zerobus::zerobus ALIAS zerobus_cpp)

target_compile_features(zerobus_cpp PUBLIC cxx_std_17)

target_include_directories(zerobus_cpp
    PUBLIC
      $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
      $<INSTALL_INTERFACE:include>
    PRIVATE
      ${CMAKE_CURRENT_SOURCE_DIR}/src)

# Linking the FFI library PUBLIC propagates both the zerobus.h include directory
# and the platform system libraries the Rust static library needs.
target_link_libraries(zerobus_cpp PUBLIC zerobus::ffi)

if(DEFINED ZEROBUS_FFI_BUILD_TARGET)
  add_dependencies(zerobus_cpp ${ZEROBUS_FFI_BUILD_TARGET})
endif()

if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
  target_compile_options(zerobus_cpp PRIVATE -Wall -Wextra)
endif()

set_target_properties(zerobus_cpp PROPERTIES
    OUTPUT_NAME zerobus_cpp
    POSITION_INDEPENDENT_CODE ON
    # Export under the same name as the in-build alias (zerobus::zerobus) so
    # consumers link the same target whether they use the build tree or the
    # installed package. Without this the export namespace would prepend to the
    # real target name, yielding zerobus::zerobus_cpp.
    EXPORT_NAME zerobus)

# ---------------------------------------------------------------------------
# Tests and examples
# ---------------------------------------------------------------------------
if(ZEROBUS_BUILD_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()

if(ZEROBUS_BUILD_EXAMPLES)
  add_subdirectory(examples)
endif()

# ---------------------------------------------------------------------------
# Install
# ---------------------------------------------------------------------------
include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

# Copy the compiled library and headers into the standard install layout, and
# record zerobus_cpp as a member of the "zerobus-targets" export set.
install(TARGETS zerobus_cpp
    EXPORT zerobus-targets
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

# Bundle the Rust C FFI archive that zerobus_cpp statically links. Without it a
# consumer could locate the C++ library but would have no symbols to satisfy its
# FFI calls. zerobus-config.cmake recreates the zerobus::ffi target from this.
get_target_property(_zerobus_ffi_location zerobus_ffi IMPORTED_LOCATION)
install(FILES "${_zerobus_ffi_location}" DESTINATION ${CMAKE_INSTALL_LIBDIR})

# Piece #1a: write zerobus-targets.cmake, which recreates the zerobus::zerobus
# target (library path, include dir, dependencies) in a consumer's build.
install(EXPORT zerobus-targets
    NAMESPACE zerobus::
    FILE zerobus-targets.cmake
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/zerobus)

# Piece #1b: generate the entry-point file find_package(zerobus) looks for,
# plus a version file so consumers can request a compatible version.
configure_package_config_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/cmake/zerobus-config.cmake.in"
    "${CMAKE_CURRENT_BINARY_DIR}/zerobus-config.cmake"
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/zerobus)
write_basic_package_version_file(
    "${CMAKE_CURRENT_BINARY_DIR}/zerobus-config-version.cmake"
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion)
install(FILES
    "${CMAKE_CURRENT_BINARY_DIR}/zerobus-config.cmake"
    "${CMAKE_CURRENT_BINARY_DIR}/zerobus-config-version.cmake"
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/zerobus)
