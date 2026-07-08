// extern "C" trampolines the core invokes to deliver acks/errors (declared in
// detail/ack_callback.hpp). Each recovers the AckCallback from user_data and
// forwards to it. Exceptions are contained — unwinding across the C FFI is UB.
#include "detail/ack_callback.hpp"

#include <cstdio>
#include <string>

#include "zerobus/ack_callback.hpp"

namespace zerobus {
namespace detail {

extern "C" void zerobus_cpp_ack_on_ack_trampoline(std::int64_t offset,
                                                  void* user_data) {
  if (user_data == nullptr) {
    return;
  }
  auto* callback = static_cast<AckCallback*>(user_data);
  try {
    callback->on_ack(offset);
  } catch (...) {
    // Contain: must not unwind across the C FFI boundary. Log so a throwing
    // callback (a user bug) leaves a signal rather than vanishing silently.
    std::fprintf(stderr,
                 "zerobus: AckCallback::on_ack threw for offset %lld; "
                 "exception swallowed at the C FFI boundary\n",
                 static_cast<long long>(offset));
  }
}

extern "C" void zerobus_cpp_ack_on_error_trampoline(std::int64_t offset,
                                                    const char* error_message,
                                                    void* user_data) {
  if (user_data == nullptr) {
    return;
  }
  auto* callback = static_cast<AckCallback*>(user_data);
  try {
    // error_message is borrowed for this call only; copy it (empty on null).
    std::string message = error_message != nullptr ? error_message : "";
    callback->on_error(offset, message);
  } catch (...) {
    // Contain: must not unwind across the C FFI boundary. Log so a throwing
    // callback (a user bug) leaves a signal rather than vanishing silently.
    std::fprintf(stderr,
                 "zerobus: AckCallback::on_error threw for offset %lld; "
                 "exception swallowed at the C FFI boundary\n",
                 static_cast<long long>(offset));
  }
}

}  // namespace detail
}  // namespace zerobus
