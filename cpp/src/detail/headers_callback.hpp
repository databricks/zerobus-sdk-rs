#ifndef ZEROBUS_DETAIL_HEADERS_CALLBACK_HPP
#define ZEROBUS_DETAIL_HEADERS_CALLBACK_HPP

#include "detail/ffi_util.hpp"

namespace zerobus {
namespace detail {

/// C trampoline matching `HeadersProviderCallback`. `user_data` must point to a
/// live `HeadersProvider`. Invokes `get_headers()` and marshals the result into
/// a `CHeaders` whose buffers are allocated so the Rust core's
/// `zerobus_free_headers` can release them (keys/values via the C string
/// allocator, the array via `malloc`). Exceptions are caught and reported via
/// `CHeaders.error_message`.
extern "C" CHeaders zerobus_cpp_headers_trampoline(void* user_data);

}  // namespace detail
}  // namespace zerobus

#endif  // ZEROBUS_DETAIL_HEADERS_CALLBACK_HPP
