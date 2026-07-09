// extern "C" trampolines the core invokes to deliver acks/errors, plus the
// AckCallback::from adapter. Each trampoline recovers the AckCallback from
// user_data and forwards to it. The AckCallback methods are noexcept, so a
// throwing callback terminates at its own throw site (with the user's stack)
// rather than unwinding across the C FFI, which is UB — the trampolines add no
// try/catch of their own. Declared in detail/ack_callback.hpp.
#include "detail/ack_callback.hpp"

#include <cstdint>
#include <functional>
#include <string>
#include <utility>

#include "zerobus/ack_callback.hpp"

namespace zerobus {

namespace {

// AckCallback backed by std::function handlers, used by AckCallback::from. The
// error handler may be empty (errors ignored). Both handlers inherit the
// noexcept contract; if one throws, std::terminate fires at the throw site.
class LambdaAckCallback : public AckCallback {
 public:
  LambdaAckCallback(
      std::function<void(std::int64_t)> on_ack,
      std::function<void(std::int64_t, const std::string&)> on_error)
      : on_ack_(std::move(on_ack)), on_error_(std::move(on_error)) {}

  void on_ack(std::int64_t offset) noexcept override {
    if (on_ack_) {
      on_ack_(offset);
    }
  }

  void on_error(std::int64_t offset,
                const std::string& error_message) noexcept override {
    if (on_error_) {
      on_error_(offset, error_message);
    }
  }

 private:
  std::function<void(std::int64_t)> on_ack_;
  std::function<void(std::int64_t, const std::string&)> on_error_;
};

}  // namespace

std::shared_ptr<AckCallback> AckCallback::from(
    std::function<void(std::int64_t)> on_ack,
    std::function<void(std::int64_t, const std::string&)> on_error) {
  return std::make_shared<LambdaAckCallback>(std::move(on_ack),
                                             std::move(on_error));
}

namespace detail {

extern "C" void zerobus_cpp_ack_on_ack_trampoline(std::int64_t offset,
                                                  void* user_data) noexcept {
  if (user_data == nullptr) {
    return;
  }
  static_cast<AckCallback*>(user_data)->on_ack(offset);
}

extern "C" void zerobus_cpp_ack_on_error_trampoline(std::int64_t offset,
                                                    const char* error_message,
                                                    void* user_data) noexcept {
  if (user_data == nullptr) {
    return;
  }
  // error_message is borrowed for this call only; copy it (empty on null).
  std::string message = error_message != nullptr ? error_message : "";
  static_cast<AckCallback*>(user_data)->on_error(offset, message);
}

}  // namespace detail
}  // namespace zerobus
