# Zerobus Pure C SDK build: the library, examples, and tests.
#
# No third-party dependencies — the SDK is the public API, its input validation,
# and error/ownership plumbing, so it needs nothing beyond a C99 compiler and
# the C standard library. The networking core (transport, OAuth, record codec,
# threading) is not implemented yet; it will add nghttp2 and OpenSSL.
cmake_minimum_required(VERSION 3.13)
project(zerobus_c LANGUAGES C VERSION 0.0.1)

set(CMAKE_C_STANDARD 99)
set(CMAKE_C_STANDARD_REQUIRED ON)
# Reject compiler extensions: build as -std=c99, not -std=gnu99, so a GNU-ism
# cannot slip in unnoticed against the documented C99 baseline.
set(CMAKE_C_EXTENSIONS OFF)
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

# Hidden visibility by default: only ZEROBUS_API symbols are exported from a
# shared build. ELF visibility only affects the dynamic symbol table, so the
# static archive still carries its external zb_* helpers as globals.
set(CMAKE_C_VISIBILITY_PRESET hidden)
set(CMAKE_VISIBILITY_INLINES_HIDDEN ON)

# Optional sanitizer, e.g. -DZEROBUS_SANITIZE=address (or thread, undefined).
set(ZEROBUS_SANITIZE "" CACHE STRING "Runtime sanitizer: address|thread|undefined")

# Treat compiler warnings as errors (used by `make lint`; off for normal builds).
option(ZEROBUS_WERROR "Treat compiler warnings as errors" OFF)

# Warning flags shared by the library, tests, and example. -Werror is appended
# only under ZEROBUS_WERROR, so a normal build surfaces warnings without failing.
set(ZEROBUS_WARN_FLAGS -Wall -Wextra)
if(ZEROBUS_WERROR)
  list(APPEND ZEROBUS_WARN_FLAGS -Werror)
endif()

set(ZEROBUS_SOURCES
  src/error.c
  src/utils.c
  src/sdk.c
  src/stream.c
)

add_library(zerobus_c STATIC ${ZEROBUS_SOURCES})
add_library(zerobus::c ALIAS zerobus_c)

target_include_directories(zerobus_c
  PUBLIC  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)

target_compile_options(zerobus_c PRIVATE ${ZEROBUS_WARN_FLAGS})

# Same sources compiled with ZB_TESTING, which gives ZB_STATIC helpers external
# linkage so unit tests can call them directly. Not shipped.
add_library(zerobus_c_testing STATIC ${ZEROBUS_SOURCES})
target_compile_definitions(zerobus_c_testing PUBLIC ZB_TESTING)
target_include_directories(zerobus_c_testing
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
         ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_compile_options(zerobus_c_testing PRIVATE ${ZEROBUS_WARN_FLAGS})

if(ZEROBUS_SANITIZE)
  foreach(lib zerobus_c zerobus_c_testing)
    target_compile_options(${lib} PUBLIC -fsanitize=${ZEROBUS_SANITIZE} -fno-omit-frame-pointer -g)
    target_link_options(${lib} PUBLIC -fsanitize=${ZEROBUS_SANITIZE})
  endforeach()
endif()

enable_testing()
add_subdirectory(examples)
add_subdirectory(tests)
