// Unit tests for the headers-provider trampoline
// (detail::zerobus_cpp_headers_trampoline): a provider's map is marshalled into
// CHeaders, the empty case yields a null array, and a throwing provider or a
// null user_data become a CHeaders.error_message rather than crossing the FFI
// boundary. Each result is freed with the Rust core's zerobus_free_headers,
// exactly as it would be in production.
#include "detail/headers_callback.hpp"

#include <map>
#include <stdexcept>
#include <string>

#include "test_harness.hpp"
#include "zerobus/headers_provider.hpp"

using namespace zerobus;

namespace {

class MapProvider : public HeadersProvider {
 public:
  std::map<std::string, std::string> headers;
  std::map<std::string, std::string> get_headers() override { return headers; }
};

class ThrowingProvider : public HeadersProvider {
 public:
  std::map<std::string, std::string> get_headers() override {
    throw std::runtime_error("provider boom");
  }
};

}  // namespace

TEST(HeadersCallback, MarshalsHeaders) {
  MapProvider provider;
  provider.headers = {{"Authorization", "Bearer abc"}, {"X-Custom", "v1"}};

  CHeaders out = detail::zerobus_cpp_headers_trampoline(&provider);
  EXPECT_EQ(out.error_message, nullptr);
  ASSERT_EQ(out.count, static_cast<std::size_t>(2));
  ASSERT_NE(out.headers, nullptr);

  std::map<std::string, std::string> got;
  for (std::size_t i = 0; i < out.count; ++i) {
    ASSERT_NE(out.headers[i].key, nullptr);
    ASSERT_NE(out.headers[i].value, nullptr);
    got[out.headers[i].key] = out.headers[i].value;
  }
  EXPECT_EQ(got["Authorization"], "Bearer abc");
  EXPECT_EQ(got["X-Custom"], "v1");

  // Freed by the Rust core's allocator, exactly as it would be in production.
  zerobus_free_headers(out);
}

TEST(HeadersCallback, EmptyHeaders) {
  MapProvider provider;  // no headers
  CHeaders out = detail::zerobus_cpp_headers_trampoline(&provider);
  EXPECT_EQ(out.count, static_cast<std::size_t>(0));
  EXPECT_EQ(out.headers, nullptr);
  EXPECT_EQ(out.error_message, nullptr);
  zerobus_free_headers(out);
}

TEST(HeadersCallback, ExceptionBecomesErrorMessage) {
  ThrowingProvider provider;
  CHeaders out = detail::zerobus_cpp_headers_trampoline(&provider);
  ASSERT_NE(out.error_message, nullptr);
  EXPECT_NE(std::string(out.error_message).find("provider boom"),
            std::string::npos);
  EXPECT_EQ(out.headers, nullptr);
  zerobus_free_headers(out);
}

TEST(HeadersCallback, NullUserDataIsError) {
  CHeaders out = detail::zerobus_cpp_headers_trampoline(nullptr);
  ASSERT_NE(out.error_message, nullptr);
  zerobus_free_headers(out);
}
