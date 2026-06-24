// The extern "C" trampoline that lets the Rust core call back into a C++
// HeadersProvider (declared in detail/headers_callback.hpp).
//
// The core invokes get_headers() through this function and the result is
// marshalled into a CHeaders whose buffers are allocated so the core's
// zerobus_free_headers can release them: the array via calloc and each
// key/value via a malloc'd C string, matching the Go wrapper's contract (see
// zerobus_free_headers in rust/ffi/src/arrow.rs). Exceptions never cross the
// boundary — they are caught and reported as CHeaders.error_message.
#include "detail/headers_callback.hpp"

#include <cstdlib>
#include <cstring>
#include <map>
#include <string>

#include "zerobus/headers_provider.hpp"

namespace zerobus {
namespace detail {

namespace {

// Duplicate a std::string into a NUL-terminated, malloc-allocated C string.
// The Rust core frees these via the C string allocator (its default global
// allocator delegates to malloc/free), matching the Go wrapper's C.CString use.
char* dup_cstring(const std::string& s) {
  char* out = static_cast<char*>(std::malloc(s.size() + 1));
  if (out == nullptr) {
    return nullptr;
  }
  std::memcpy(out, s.data(), s.size());
  out[s.size()] = '\0';
  return out;
}

CHeaders make_error(const std::string& message) {
  CHeaders headers{};
  headers.headers = nullptr;
  headers.count = 0;
  headers.error_message = dup_cstring(message);
  return headers;
}

void free_marshaled_headers(CHeader* headers, std::size_t count) {
  if (headers == nullptr) {
    return;
  }
  for (std::size_t i = 0; i < count; ++i) {
    std::free(headers[i].key);
    std::free(headers[i].value);
  }
  std::free(headers);
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

  auto* arr =
      static_cast<CHeader*>(std::calloc(headers.size(), sizeof(CHeader)));
  if (arr == nullptr) {
    return make_error("out of memory marshalling headers");
  }

  std::size_t i = 0;
  for (const auto& kv : headers) {
    arr[i].key = dup_cstring(kv.first);
    arr[i].value = dup_cstring(kv.second);
    if (arr[i].key == nullptr || arr[i].value == nullptr) {
      free_marshaled_headers(arr, i + 1);
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
