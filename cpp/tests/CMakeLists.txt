# Tests for the Zerobus C++ SDK.
#
# Dependency-free by design: the SDK build pulls in no test framework, so each
# test is a plain executable that returns non-zero on failure and is registered
# with CTest. Run via `make test` (ctest) from the cpp/ directory.

# config_defaults_test reaches into the private src/detail headers (config_convert
# and the generated zerobus.h via ffi_util), so it links the library target and
# adds src/ to its include path. Linking zerobus::zerobus also pulls in the FFI
# archive that defines zerobus_get_default_config().
# Tests reach into private src/detail headers, so link the library (which also
# pulls in the FFI archive) and add src/ to the include path.
function(zerobus_add_test name)
  add_executable(${name} ${name}.cpp)
  target_link_libraries(${name} PRIVATE zerobus::zerobus)
  target_include_directories(${name} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../src)
  if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(${name} PRIVATE -Wall -Wextra)
  endif()
  add_test(NAME ${name} COMMAND ${name})
endfunction()

zerobus_add_test(config_defaults_test)
zerobus_add_test(embedded_nul_test)
