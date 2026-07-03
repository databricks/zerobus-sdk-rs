# Tests for the Zerobus C++ SDK.
#
# Dependency-free by design: the SDK build pulls in no test framework, so each
# test is a plain executable that returns non-zero on failure and is registered
# with CTest. Run via `make test` (ctest) from the cpp/ directory.

# config_defaults_test reaches into the private src/detail headers (config_convert
# and the generated zerobus.h via ffi_util), so it links the library target and
# adds src/ to its include path. Linking zerobus::zerobus also pulls in the FFI
# archive that defines zerobus_get_default_config().
add_executable(config_defaults_test config_defaults_test.cpp)
target_link_libraries(config_defaults_test PRIVATE zerobus::zerobus)
target_include_directories(config_defaults_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/../src)
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
  target_compile_options(config_defaults_test PRIVATE -Wall -Wextra)
endif()
add_test(NAME config_defaults_test COMMAND config_defaults_test)
