// Verifies the C++ ack-callback wiring: to_c() installs trampolines only when a
// callback is set, wait_forever maps correctly, and the trampolines dispatch,
// tolerate null user_data / null message, and contain exceptions. Returns
// non-zero on failure (dependency-free, like the other tests).

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

void test_wait_forever_clears_presence_flag() {
  // wait_forever must clear the presence flag (Rust reads None => drain
  // forever) and win over an explicit finite budget.
  zerobus::StreamOptions opts;
  opts.callback_wait_forever = true;
  opts.callback_max_wait_time_ms = 1234;  // ignored when waiting forever
  zerobus::CStreamConfigurationOptions c = zerobus::detail::to_c(opts);
  check("wait_forever => presence flag cleared",
        c.has_callback_max_wait_time_ms == false);

  // Without wait_forever, a finite budget is still honored.
  opts.callback_wait_forever = false;
  c = zerobus::detail::to_c(opts);
  check("finite budget => presence flag set",
        c.has_callback_max_wait_time_ms == true);
  check("finite budget => value forwarded",
        c.callback_max_wait_time_ms == 1234);
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
  check("on_error still invoked on null message", cb.error_count == 2);
}

void test_null_user_data_is_ignored() {
  // Null user_data must hit the early-return guard rather than dereferencing
  // it. There is no callback to observe (that is the point): reaching the end
  // without crashing is the assertion, so there is nothing to check().
  zerobus::detail::zerobus_cpp_ack_on_ack_trampoline(1, nullptr);
  zerobus::detail::zerobus_cpp_ack_on_error_trampoline(1, "x", nullptr);
}

void test_exceptions_are_contained() {
  ThrowingCallback cb;
  // Neither call may propagate the exception out of the trampoline: returning
  // here (rather than terminating on an escaped throw) is the assertion.
  zerobus::detail::zerobus_cpp_ack_on_ack_trampoline(1, &cb);
  zerobus::detail::zerobus_cpp_ack_on_error_trampoline(1, "x", &cb);
}

}  // namespace

int main() {
  test_to_c_installs_only_when_set();
  test_wait_forever_clears_presence_flag();
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
