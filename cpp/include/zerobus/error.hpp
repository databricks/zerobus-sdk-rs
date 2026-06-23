#ifndef ZEROBUS_ERROR_HPP
#define ZEROBUS_ERROR_HPP

#include <stdexcept>
#include <string>

namespace zerobus {

/// Exception thrown by all Zerobus SDK operations on failure.
///
/// Mirrors the `CResult` carried across the C FFI boundary: a human-readable
/// message plus a retryability flag. `is_retryable()` reflects the Rust core's
/// `ZerobusError::is_retryable()` classification — callers may retry the
/// operation when it returns `true`.
class ZerobusException : public std::runtime_error {
 public:
  ZerobusException(std::string message, bool is_retryable)
      : std::runtime_error(std::move(message)), is_retryable_(is_retryable) {}

  /// Whether the underlying error is transient and the operation may be
  /// retried.
  bool is_retryable() const noexcept { return is_retryable_; }

 private:
  bool is_retryable_;
};

}  // namespace zerobus

#endif  // ZEROBUS_ERROR_HPP
