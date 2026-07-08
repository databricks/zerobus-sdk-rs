#ifndef ZEROBUS_DETAIL_ACK_CALLBACK_HPP
#define ZEROBUS_DETAIL_ACK_CALLBACK_HPP

#include <cstdint>

namespace zerobus {
namespace detail {

/// C trampolines matching the `ack_on_ack` / `ack_on_error` function-pointer
/// fields of `CStreamConfigurationOptions`. `user_data` must point to a live
/// `AckCallback` (the `StreamOptions::ack_callback` the owning `Stream` keeps
/// alive). Each dispatches to the matching `AckCallback` method. Exceptions are
/// caught and dropped rather than allowed to unwind across the C boundary; a
/// null `user_data` is ignored.
extern "C" void zerobus_cpp_ack_on_ack_trampoline(std::int64_t offset_id,
                                                  void* user_data);

extern "C" void zerobus_cpp_ack_on_error_trampoline(std::int64_t offset_id,
                                                    const char* error_message,
                                                    void* user_data);

}  // namespace detail
}  // namespace zerobus

#endif  // ZEROBUS_DETAIL_ACK_CALLBACK_HPP
