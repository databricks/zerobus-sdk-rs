#ifndef ZEROBUS_DETAIL_HEADERS_CALLBACK_HPP
#define ZEROBUS_DETAIL_HEADERS_CALLBACK_HPP

#include "detail/ffi_util.hpp"

namespace zerobus {
namespace detail {

/// C trampoline matching `HeadersProviderCallback`. `user_data` must point to a
/// heap-allocated `std::shared_ptr<HeadersProvider>` (owned by the FFI; see
/// `zerobus_cpp_headers_free`). Invokes `get_headers()` and marshals the result
/// into a `CHeaders` whose buffers are allocated so the Rust core's
/// `zerobus_free_headers` can release them (keys/values via the C string
/// allocator, the array via `malloc`). Exceptions are caught and reported via
/// `CHeaders.error_message`.
extern "C" CHeaders zerobus_cpp_headers_trampoline(void* user_data);

/// C trampoline matching the FFI's provider `free_user_data` callback. Deletes
/// the heap-allocated `std::shared_ptr<HeadersProvider>` that `user_data`
/// points to, releasing the provider. The FFI invokes it exactly once — after
/// any in-flight `get_headers()` has returned — which is what makes it safe to
/// destroy the provider (see `headers_provider.hpp`). `noexcept`: it must not
/// unwind across the C boundary.
extern "C" void zerobus_cpp_headers_free(void* user_data) noexcept;

}  // namespace detail
}  // namespace zerobus

#endif  // ZEROBUS_DETAIL_HEADERS_CALLBACK_HPP
