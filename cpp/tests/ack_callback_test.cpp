// Verifies the C++ ack-callback wiring: to_c() installs trampolines only when a
// callback is set, the wait policy maps correctly, the trampolines dispatch and
// tolerate null user_data / null message, and AckCallback::from adapts
// std::functions. Returns non-zero on failure (dependency-free, like the other
// tests).

#include "detail/ack_callback.hpp"

#include <cstdint>
#include <cstdio>
#include <memory>
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

// Records what the trampolines deliver. Fields are non-atomic: safe for these
// synchronous unit tests. Live-stream teardown (callbacks driven from the core's
// background thread) is covered by the Rust-side ack-callback teardown tests,
// which use a thread-safe fixture.
class RecordingCallback : public zerobus::AckCallback {
 public:
  std::int64_t last_ack = -1;
  std::int64_t last_error_offset = -1;
  std::string last_error_message;
  int ack_count = 0;
  int error_count = 0;

  void on_ack(std::int64_t offset) noexcept override {
    last_ack = offset;
    ++ack_count;
  }
  void on_error(std::int64_t offset,
                const std::string& message) noexcept override {
    last_error_offset = offset;
    last_error_message = message;
    ++error_count;
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

void test_wait_policy_maps_to_ffi() {
  // forever() clears the presence flag (Rust reads None => drain forever).
  zerobus::StreamOptions opts;
  opts.callback_wait_policy = zerobus::CallbackWaitPolicy::forever();
  zerobus::CStreamConfigurationOptions c = zerobus::detail::to_c(opts);
  check("forever => presence flag cleared",
        c.has_callback_max_wait_time_ms == false);

  // duration(ms) sets an explicit finite budget.
  opts.callback_wait_policy = zerobus::CallbackWaitPolicy::duration(1234);
  c = zerobus::detail::to_c(opts);
  check("duration => presence flag set",
        c.has_callback_max_wait_time_ms == true);
  check("duration => value forwarded", c.callback_max_wait_time_ms == 1234);

  // use_default() leaves the finite FFI seed untouched (presence stays set, as
  // config_defaults_test also pins).
  opts.callback_wait_policy = zerobus::CallbackWaitPolicy::use_default();
  c = zerobus::detail::to_c(opts);
  check(
      "default => presence flag left at FFI seed",
      c.has_callback_max_wait_time_ms ==
          zerobus::zerobus_get_default_config().has_callback_max_wait_time_ms);
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

void test_from_adapter_dispatches() {
  // AckCallback::from wraps std::functions; the trampolines dispatch to them
  // just like a subclass.
  std::int64_t acked = -1;
  std::int64_t err_offset = -1;
  std::string err_message;
  auto cb = zerobus::AckCallback::from(
      [&](std::int64_t offset) noexcept { acked = offset; },
      [&](std::int64_t offset, const std::string& msg) noexcept {
        err_offset = offset;
        err_message = msg;
      });
  zerobus::detail::zerobus_cpp_ack_on_ack_trampoline(5, cb.get());
  zerobus::detail::zerobus_cpp_ack_on_error_trampoline(6, "bad", cb.get());
  check("from: on_ack dispatched", acked == 5);
  check("from: on_error offset dispatched", err_offset == 6);
  check("from: on_error message dispatched", err_message == "bad");
}

void test_from_adapter_omitted_error_handler() {
  // The error handler is optional; a null one must be tolerated, not crash.
  bool acked = false;
  auto cb =
      zerobus::AckCallback::from([&](std::int64_t) noexcept { acked = true; });
  zerobus::detail::zerobus_cpp_ack_on_ack_trampoline(1, cb.get());
  zerobus::detail::zerobus_cpp_ack_on_error_trampoline(1, "x", cb.get());
  check("from: on_ack fired with omitted error handler", acked);
}

}  // namespace

int main() {
  test_to_c_installs_only_when_set();
  test_wait_policy_maps_to_ffi();
  test_trampolines_dispatch();
  test_null_user_data_is_ignored();
  test_from_adapter_dispatches();
  test_from_adapter_omitted_error_handler();

  if (g_failures != 0) {
    std::fprintf(stderr, "%d ack-callback check(s) failed.\n", g_failures);
    return 1;
  }
  std::printf("ack callback wiring OK\n");
  return 0;
}
