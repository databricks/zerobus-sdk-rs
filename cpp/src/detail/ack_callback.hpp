#ifndef ZEROBUS_DETAIL_ACK_CALLBACK_HPP
#define ZEROBUS_DETAIL_ACK_CALLBACK_HPP

#include <cstdint>

namespace zerobus {
namespace detail {

/// C trampolines for the `ack_on_ack` / `ack_on_error` fields of
/// `CStreamConfigurationOptions`. Each dispatches `user_data` (a live
/// `AckCallback`) to the matching method, ignoring a null `user_data`.
/// `noexcept` because unwinding across the C FFI is UB: an escaping exception
/// must `std::terminate`, not propagate.
extern "C" void zerobus_cpp_ack_on_ack_trampoline(std::int64_t offset,
                                                  void* user_data) noexcept;

extern "C" void zerobus_cpp_ack_on_error_trampoline(std::int64_t offset,
                                                    const char* error_message,
                                                    void* user_data) noexcept;

}  // namespace detail
}  // namespace zerobus

#endif  // ZEROBUS_DETAIL_ACK_CALLBACK_HPP
