// The extern "C" trampolines that let the Rust core call back into a C++
// AckCallback (declared in detail/ack_callback.hpp).
//
// The core delivers per-record acks and terminal errors through these function
// pointers, which it invokes on a dedicated, serialized background task. Each
// trampoline recovers the AckCallback from user_data (the StreamOptions
// ack_callback the owning Stream keeps alive for its whole lifetime) and
// forwards to the matching virtual method.
//
// Exceptions never cross the boundary: unwinding into the Rust core across the
// C FFI is undefined behavior, so any exception a user callback throws is
// caught and dropped here. Users must handle their own errors inside
// on_ack/on_error; the catch is a safety net, not an error channel.
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
    // Contain: an exception must not unwind across the C FFI boundary.
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
    // error_message is valid only for this call and may in principle be null;
    // copy into an owned std::string (empty on null) before handing it over.
    std::string message = error_message != nullptr ? error_message : "";
    callback->on_error(offset_id, message);
  } catch (...) {
    // Contain: an exception must not unwind across the C FFI boundary.
  }
}

}  // namespace detail
}  // namespace zerobus
