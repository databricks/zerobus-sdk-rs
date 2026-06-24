#ifndef ZEROBUS_DETAIL_FFI_UTIL_HPP
#define ZEROBUS_DETAIL_FFI_UTIL_HPP

// The cbindgen-generated C header. It declares everything inside
// `namespace zerobus { extern "C" { ... } }`, so the C symbols are referenced
// from this SDK's `zerobus` namespace unqualified.
#include <string>
#include <utility>

#include "zerobus.h"
#include "zerobus/error.hpp"

namespace zerobus {
namespace detail {

/// Owns a `CResult` for the duration of one FFI call and converts failure into
/// a `ZerobusException`. The error string allocated by the Rust core is always
/// freed (via `zerobus_free_error_message`) before throwing.
class ResultGuard {
 public:
  ResultGuard() : result_{} {}

  ~ResultGuard() {
    // Defensive: if the message was set but never consumed (e.g. the caller
    // chose not to call throw_if_error after an FFI call that wrote one),
    // still free it to avoid a leak.
    if (result_.error_message != nullptr) {
      zerobus_free_error_message(result_.error_message);
      result_.error_message = nullptr;
    }
  }

  ResultGuard(const ResultGuard&) = delete;
  ResultGuard& operator=(const ResultGuard&) = delete;

  CResult* ptr() { return &result_; }

  /// Throw a ZerobusException if the FFI reported failure. Frees the C error
  /// string either way. Safe to call multiple times.
  void throw_if_error() {
    if (!result_.success) {
      std::string message = result_.error_message != nullptr
                                ? std::string(result_.error_message)
                                : std::string("unknown Zerobus error");
      bool retryable = result_.is_retryable;
      if (result_.error_message != nullptr) {
        zerobus_free_error_message(result_.error_message);
        result_.error_message = nullptr;
      }
      throw ZerobusException(std::move(message), retryable);
    }
    // Success: there should be no message, but free defensively.
    if (result_.error_message != nullptr) {
      zerobus_free_error_message(result_.error_message);
      result_.error_message = nullptr;
    }
  }

 private:
  CResult result_;
};

}  // namespace detail
}  // namespace zerobus

#endif  // ZEROBUS_DETAIL_FFI_UTIL_HPP
