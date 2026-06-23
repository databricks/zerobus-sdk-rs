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
      static_cast<CHeader*>(std::malloc(sizeof(CHeader) * headers.size()));
  if (arr == nullptr) {
    return make_error("out of memory marshalling headers");
  }

  std::size_t i = 0;
  for (const auto& kv : headers) {
    arr[i].key = dup_cstring(kv.first);
    arr[i].value = dup_cstring(kv.second);
    ++i;
  }

  result.headers = arr;
  result.count = headers.size();
  return result;
}

}  // namespace detail
}  // namespace zerobus
