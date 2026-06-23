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
    POSITION_INDEPENDENT_CODE ON)

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
install(TARGETS zerobus_cpp
    EXPORT zerobus-targets
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
