# The test suite uses a tiny in-repo harness (test_harness.hpp) instead of an
# external framework, so the build is fully hermetic — no vendored framework, no
# package manager, no network. CI works on locked-down runners as a result.
add_executable(zerobus_tests
    test_main.cpp
    error_test.cpp
    config_test.cpp
    headers_callback_test.cpp
    proto_schema_test.cpp
    sdk_test.cpp)

target_link_libraries(zerobus_tests PRIVATE zerobus::zerobus)

# The harness header lives here; tests also reach internal detail/ headers.
target_include_directories(zerobus_tests PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/../src)

# One CTest entry: the binary runs every TEST and exits non-zero on any failure.
add_test(NAME zerobus_tests COMMAND zerobus_tests)
