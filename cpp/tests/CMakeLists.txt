# GoogleTest: prefer a system install, otherwise fetch a pinned release.
find_package(GTest QUIET)

if(NOT GTest_FOUND)
  include(FetchContent)
  FetchContent_Declare(
      googletest
      URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.tar.gz
      URL_HASH SHA256=8ad598c73ad796e0d8280b082cebd82a630d73e73cd3c70057938a6501bba5d7)
  # Don't override the parent project's compiler/runtime settings on Windows.
  set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
  FetchContent_MakeAvailable(googletest)
endif()

add_executable(zerobus_tests
    error_test.cpp
    config_test.cpp
    headers_callback_test.cpp
    proto_schema_test.cpp
    sdk_test.cpp)

target_link_libraries(zerobus_tests PRIVATE zerobus::zerobus GTest::gtest_main)

# Tests reach into internal detail headers (config_convert, headers_callback).
target_include_directories(zerobus_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../src)

include(GoogleTest)
gtest_discover_tests(zerobus_tests)
