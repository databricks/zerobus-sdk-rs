// Verifies the C++ ack-callback wiring:
//  1. to_c() installs the trampolines and points ack_user_data at the callback
//     only when StreamOptions::ack_callback is set.
//  2. The extern "C" trampolines dispatch to the right AckCallback method with
//     the right arguments, tolerate a null user_data, copy the error message,
//     and contain exceptions thrown by user callbacks (they must not escape
//     across the C FFI boundary).
//
// Dependency-free like the other tests: returns non-zero on failure.

#include "detail/ack_callback.hpp"

#include <cstdint>
#include <cstdio>
#include <memory>
#include <stdexcept>
#include <string>

#include "detail/config_convert.hpp"
#include "zerobus/ack_callback.hpp"
#include "zerobus/config.hpp"

namespace {

int g_failures = 0;

void check(const char* what, bool ok) {
  if (!ok) {
    std::fprintf(stderr, "FAIL: %s\n", what);
    ++g_failures;
  }
}

// Records what the trampolines deliver.
class RecordingCallback : public zerobus::AckCallback {
 public:
  std::int64_t last_ack = -1;
  std::int64_t last_error_offset = -1;
  std::string last_error_message;
  int ack_count = 0;
  int error_count = 0;

  void on_ack(std::int64_t offset) override {
    last_ack = offset;
    ++ack_count;
  }
  void on_error(std::int64_t offset, const std::string& message) override {
    last_error_offset = offset;
    last_error_message = message;
    ++error_count;
  }
};

// Always throws, to prove exceptions are contained at the boundary.
class ThrowingCallback : public zerobus::AckCallback {
 public:
  void on_ack(std::int64_t) override { throw std::runtime_error("boom"); }
  void on_error(std::int64_t, const std::string&) override {
    throw std::runtime_error("boom");
  }
};

void test_to_c_installs_only_when_set() {
  // No callback: fields stay null.
  zerobus::StreamOptions opts;
  zerobus::CStreamConfigurationOptions c = zerobus::detail::to_c(opts);
  check("no callback => ack_on_ack null", c.ack_on_ack == nullptr);
  check("no callback => ack_on_error null", c.ack_on_error == nullptr);
  check("no callback => ack_user_data null", c.ack_user_data == nullptr);

  // Callback set: trampolines installed, user_data points at the callback.
  auto cb = std::make_shared<RecordingCallback>();
  opts.ack_callback = cb;
  c = zerobus::detail::to_c(opts);
  check("callback => ack_on_ack set", c.ack_on_ack != nullptr);
  check("callback => ack_on_error set", c.ack_on_error != nullptr);
  check("callback => ack_user_data points at callback",
        c.ack_user_data == cb.get());
}

void test_trampolines_dispatch() {
  RecordingCallback cb;

  zerobus::detail::zerobus_cpp_ack_on_ack_trampoline(42, &cb);
  check("on_ack forwards offset", cb.last_ack == 42);
  check("on_ack called once", cb.ack_count == 1);

  zerobus::detail::zerobus_cpp_ack_on_error_trampoline(7, "kaboom", &cb);
  check("on_error forwards offset", cb.last_error_offset == 7);
  check("on_error forwards message", cb.last_error_message == "kaboom");
  check("on_error called once", cb.error_count == 1);

  // A null error_message must become an empty string, not crash.
  zerobus::detail::zerobus_cpp_ack_on_error_trampoline(8, nullptr, &cb);
  check("on_error null message => empty", cb.last_error_message.empty());
  check("on_error offset still forwarded", cb.last_error_offset == 8);
}

void test_null_user_data_is_ignored() {
  // Must not dereference null; simply returns.
  zerobus::detail::zerobus_cpp_ack_on_ack_trampoline(1, nullptr);
  zerobus::detail::zerobus_cpp_ack_on_error_trampoline(1, "x", nullptr);
  check("null user_data tolerated", true);  // reached here => no crash
}

void test_exceptions_are_contained() {
  ThrowingCallback cb;
  // Neither call may propagate the exception out of the trampoline.
  zerobus::detail::zerobus_cpp_ack_on_ack_trampoline(1, &cb);
  zerobus::detail::zerobus_cpp_ack_on_error_trampoline(1, "x", &cb);
  check("exceptions contained at boundary", true);  // reached here => contained
}

}  // namespace

int main() {
  test_to_c_installs_only_when_set();
  test_trampolines_dispatch();
  test_null_user_data_is_ignored();
  test_exceptions_are_contained();

  if (g_failures != 0) {
    std::fprintf(stderr, "%d ack-callback check(s) failed.\n", g_failures);
    return 1;
  }
  std::printf("ack callback wiring OK\n");
  return 0;
}
