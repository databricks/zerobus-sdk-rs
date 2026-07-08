#ifndef ZEROBUS_DETAIL_ACK_CALLBACK_HPP
#define ZEROBUS_DETAIL_ACK_CALLBACK_HPP

#include <cstdint>

namespace zerobus {
namespace detail {

/// C trampolines for the `ack_on_ack` / `ack_on_error` fields of
/// `CStreamConfigurationOptions`. `user_data` points to a live `AckCallback`;
/// each dispatches to the matching method. Exceptions are contained and a null
/// `user_data` is ignored.
extern "C" void zerobus_cpp_ack_on_ack_trampoline(std::int64_t offset_id,
                                                  void* user_data);

extern "C" void zerobus_cpp_ack_on_error_trampoline(std::int64_t offset_id,
                                                    const char* error_message,
                                                    void* user_data);

}  // namespace detail
}  // namespace zerobus

#endif  // ZEROBUS_DETAIL_ACK_CALLBACK_HPP
