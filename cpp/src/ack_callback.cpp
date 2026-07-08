// extern "C" trampolines the core invokes to deliver acks/errors (declared in
// detail/ack_callback.hpp). Each recovers the AckCallback from user_data and
// forwards to it. Exceptions are contained — unwinding across the C FFI is UB.
#include "detail/ack_callback.hpp"

#include <string>

#include "zerobus/ack_callback.hpp"

namespace zerobus {
namespace detail {

extern "C" void zerobus_cpp_ack_on_ack_trampoline(std::int64_t offset_id,
                                                  void* user_data) {
  if (user_data == nullptr) {
    return;
  }
  auto* callback = static_cast<AckCallback*>(user_data);
  try {
    callback->on_ack(offset_id);
  } catch (...) {
    // Contain: must not unwind across the C FFI boundary.
  }
}

extern "C" void zerobus_cpp_ack_on_error_trampoline(std::int64_t offset_id,
                                                    const char* error_message,
                                                    void* user_data) {
  if (user_data == nullptr) {
    return;
  }
  auto* callback = static_cast<AckCallback*>(user_data);
  try {
    // error_message is borrowed for this call only; copy it (empty on null).
    std::string message = error_message != nullptr ? error_message : "";
    callback->on_error(offset_id, message);
  } catch (...) {
    // Contain: must not unwind across the C FFI boundary.
  }
}

}  // namespace detail
}  // namespace zerobus
