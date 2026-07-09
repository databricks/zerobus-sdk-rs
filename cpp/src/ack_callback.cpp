// extern "C" trampolines the core invokes to deliver acks/errors. Each recovers
// the AckCallback from user_data and forwards to it, containing exceptions
// (unwinding across the C FFI is UB). Declared in detail/ack_callback.hpp.
#include "detail/ack_callback.hpp"

#include <cinttypes>
#include <cstdio>
#include <string>

#include "zerobus/ack_callback.hpp"

namespace zerobus {
namespace detail {

extern "C" void zerobus_cpp_ack_on_ack_trampoline(std::int64_t offset,
                                                  void* user_data) noexcept {
  if (user_data == nullptr) {
    return;
  }
  auto* callback = static_cast<AckCallback*>(user_data);
  try {
    callback->on_ack(offset);
  } catch (...) {
    // Contain (can't unwind across the FFI) but log, so a throwing callback bug
    // leaves a signal.
    std::fprintf(stderr,
                 "zerobus: AckCallback::on_ack threw for offset %" PRId64
                 "; exception swallowed at the C FFI boundary\n",
                 offset);
  }
}

extern "C" void zerobus_cpp_ack_on_error_trampoline(std::int64_t offset,
                                                    const char* error_message,
                                                    void* user_data) noexcept {
  if (user_data == nullptr) {
    return;
  }
  auto* callback = static_cast<AckCallback*>(user_data);
  try {
    // error_message is borrowed for this call only; copy it (empty on null).
    std::string message = error_message != nullptr ? error_message : "";
    callback->on_error(offset, message);
  } catch (...) {
    // See on_ack trampoline: contain but log.
    std::fprintf(stderr,
                 "zerobus: AckCallback::on_error threw for offset %" PRId64
                 "; exception swallowed at the C FFI boundary\n",
                 offset);
  }
}

}  // namespace detail
}  // namespace zerobus
