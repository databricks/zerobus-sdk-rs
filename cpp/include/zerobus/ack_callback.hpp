#ifndef ZEROBUS_ACK_CALLBACK_HPP
#define ZEROBUS_ACK_CALLBACK_HPP

#include <cstdint>
#include <string>

namespace zerobus {

/// Async ack/error notifications, so callers track durability without blocking
/// in `wait_for_offset()` / `flush()`. Register via
/// `StreamOptions::ack_callback`.
///
/// `on_ack` fires once per record, in monotonic offset order (offset `N` =>
/// all `<= N` acked); `on_error` fires per unacked record on terminal failure,
/// which may also surface from `ingest`/`flush`/`wait_for_offset()`. Callbacks
/// run serialized on a background task, possibly on another thread: synchronize
/// shared state, keep them light, and don't call back into the owning `Stream`
/// (that is concurrent use of a non-thread-safe object).
///
/// Lifetime: the `Stream` holds a `shared_ptr` for its own lifetime, but that
/// is not always enough. A callback still running when `close()` hits
/// `callback_max_wait_time_ms` keeps running (abort only cancels at an await,
/// not in synchronous user code) and then touches a freed object once the
/// `Stream` is destroyed. Avoid this by keeping callbacks well under the
/// budget, or keeping the callback alive past the `Stream` (which prevents the
/// use-after-free, though a callback may still run after `close()` returns).
/// Setting `StreamOptions::callback_wait_forever` is the only option that also
/// guarantees no callback is still running once `close()` returns: `close()`
/// blocks until every in-flight callback finishes.
///
/// Don't throw: unwinding across the C FFI boundary is UB. Escaping exceptions
/// are caught and logged.
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
