#ifndef ZEROBUS_ACK_CALLBACK_HPP
#define ZEROBUS_ACK_CALLBACK_HPP

#include <cstdint>
#include <string>

namespace zerobus {

/// Receives async ack/error notifications, so callers track durability without
/// blocking in `wait_for_offset()` / `flush()`. Register via
/// `StreamOptions::ack_callback`.
///
/// `on_ack` fires once per record in offset order, monotonic (offset `N`
/// implies all `<= N` acked); `on_error` fires per record left unacked on
/// terminal failure. Callbacks run serialized on a background task and may run
/// on a different thread than the stream: synchronize shared state and keep
/// them light. Don't call back into the owning `Stream` (ingest/flush/close)
/// from a callback — that is concurrent use of a non-thread-safe object.
///
/// Lifetime: the `Stream` holds a `shared_ptr` to the callback for its own
/// lifetime. This is necessary but not always sufficient: a callback still
/// running when `close()` hits `callback_max_wait_time_ms` can be invoked after
/// `close()` returns, and on a freed object if the `Stream` is then destroyed.
/// Keep callbacks well under that budget, or keep the callback alive past the
/// `Stream`.
///
/// Don't throw — unwinding across the C FFI boundary is undefined behavior; the
/// SDK catches and logs any exception that escapes.
class AckCallback {
 public:
  virtual ~AckCallback() = default;

  /// Called when the record at @p offset has been durably acknowledged.
  virtual void on_ack(std::int64_t offset) = 0;

  /// Called when the record at @p offset failed terminally.
  ///
  /// @param offset The logical offset of the failed record.
  /// @param error_message Human-readable error text from the core.
  virtual void on_error(std::int64_t offset,
                        const std::string& error_message) = 0;
};

}  // namespace zerobus

#endif  // ZEROBUS_ACK_CALLBACK_HPP
