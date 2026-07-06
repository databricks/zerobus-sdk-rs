// The extern "C" trampoline that lets the Rust core call back into a C++
// HeadersProvider (declared in detail/headers_callback.hpp).
//
// The core invokes get_headers() through this function and the result is
// marshalled into a CHeaders. Its buffers are allocated through the Rust core
// (zerobus_alloc_header_array for the array, zerobus_alloc_cstring for each
// key/value/error string) rather than the C++ allocator, so the allocate and
// the matching zerobus_free_headers happen in the same library. This avoids a
// cross-allocator free: on Windows this statically linked library and the C++
// translation units can resolve to different CRT heaps, and freeing a
// C++-malloc'd pointer through Rust's allocator would corrupt the heap.
// Exceptions never cross the boundary — they are caught and reported as
// CHeaders.error_message.
#include "detail/headers_callback.hpp"

#include <cstdint>
#include <map>
#include <string>

#include "zerobus/headers_provider.hpp"

namespace zerobus {
namespace detail {

namespace {

// Allocate a Rust-owned C string copy of `s` via the FFI, so
// zerobus_free_headers frees it with the matching allocator. Returns null on
// OOM or interior NUL.
char* alloc_cstring(const std::string& s) {
  return zerobus_alloc_cstring(reinterpret_cast<const std::uint8_t*>(s.data()),
                               s.size());
}

CHeaders make_error(const std::string& message) {
  CHeaders headers{};
  headers.headers = nullptr;
  headers.count = 0;
  headers.error_message = alloc_cstring(message);
  if (headers.error_message == nullptr) {
    // Allocating the message failed (OOM) or it held an interior NUL. Fall back
    // to a non-null empty marker: the Rust core treats a null error_message as
    // "success with no headers", so without this the original error would be
    // swallowed and the request would proceed unauthenticated. The text is
    // lost, but the error is still signalled. (free'd by zerobus_free_headers.)
    headers.error_message = zerobus_alloc_cstring(nullptr, 0);
  }
  return headers;
}

// Free a partially populated header array via the Rust core, matching how it
// was allocated. The array is zero-initialised, so any key/value not yet filled
// in is null and skipped; `count` need only cover the entries touched so far.
void free_partial_headers(CHeader* headers, std::size_t count) {
  CHeaders wrapper{};
  wrapper.headers = headers;
  wrapper.count = count;
  wrapper.error_message = nullptr;
  zerobus_free_headers(wrapper);
}

}  // namespace

extern "C" CHeaders zerobus_cpp_headers_trampoline(void* user_data) {
  if (user_data == nullptr) {
    return make_error("null headers provider");
  }
  auto* provider = static_cast<HeadersProvider*>(user_data);

  std::map<std::string, std::string> headers;
  try {
    headers = provider->get_headers();
  } catch (const std::exception& e) {
    return make_error(e.what());
  } catch (...) {
    return make_error("headers provider threw a non-std::exception");
  }

  CHeaders result{};
  result.error_message = nullptr;
  result.count = 0;
  result.headers = nullptr;

  if (headers.empty()) {
    return result;
  }

  CHeader* arr = zerobus_alloc_header_array(headers.size());
  if (arr == nullptr) {
    return make_error("out of memory marshalling headers");
  }

  std::size_t i = 0;
  for (const auto& kv : headers) {
    // Header keys/values cross the FFI as NUL-terminated C strings. A string
    // containing an embedded NUL would be silently truncated at the first NUL
    // on the Rust side, so reject it explicitly rather than send corrupt
    // metadata. (arr[0..i) are already populated; free those.)
    if (kv.first.find('\0') != std::string::npos ||
        kv.second.find('\0') != std::string::npos) {
      free_partial_headers(arr, i);
      return make_error("header key or value contains an embedded NUL byte");
    }
    arr[i].key = alloc_cstring(kv.first);
    arr[i].value = alloc_cstring(kv.second);
    if (arr[i].key == nullptr || arr[i].value == nullptr) {
      free_partial_headers(arr, i + 1);
      return make_error("out of memory marshalling headers");
    }
    ++i;
  }

  result.headers = arr;
  result.count = headers.size();
  return result;
}

}  // namespace detail
}  // namespace zerobus
