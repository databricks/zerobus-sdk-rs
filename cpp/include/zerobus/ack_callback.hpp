#ifndef ZEROBUS_ACK_CALLBACK_HPP
#define ZEROBUS_ACK_CALLBACK_HPP

#include <cstdint>
#include <functional>
#include <memory>
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
/// Both methods are `noexcept`: unwinding across the C FFI boundary is UB, so
/// an escaping exception calls `std::terminate` at the throw site. Handle your
/// own errors inside the callback.
///
/// Lifetime (canonical description; other doc comments defer here). The
/// `Stream` holds a `shared_ptr` for its own lifetime, but that is not always
/// enough. A callback still running when `close()` reaches its wait budget (see
/// `StreamOptions::callback_wait_policy`) keeps running — abort only cancels at
/// an await, not in synchronous user code — and then touches a freed object
/// once the `Stream` is destroyed. Avoid this by keeping callbacks well under
/// the budget, or by keeping the callback alive past the `Stream` (which
/// prevents the use-after-free, though a callback may still run after `close()`
/// returns). `CallbackWaitPolicy::forever()` is the only setting that also
/// guarantees no callback is still running once `close()` returns: `close()`
/// blocks until every in-flight callback finishes.
class AckCallback {
 public:
  virtual ~AckCallback() = default;

  /// Called when the record at @p offset has been durably acknowledged.
  virtual void on_ack(std::int64_t offset) noexcept = 0;

  /// Called when the record at @p offset failed terminally.
  ///
  /// @param offset The logical offset of the failed record.
  /// @param error_message Human-readable error text from the core.
  virtual void on_error(std::int64_t offset,
                        const std::string& error_message) noexcept = 0;

  /// Adapt free functions / lambdas into an `AckCallback`, so callers don't
  /// have to declare a subclass for the common case:
  ///
  /// ```
  /// opts.ack_callback = zerobus::AckCallback::from(
  ///     [](std::int64_t offset) noexcept { /* durable up to offset */ },
  ///     [](std::int64_t offset, const std::string& msg) noexcept { /* failed
  ///     */ });
  /// ```
  ///
  /// Both handlers must not throw (see the class contract). The error handler
  /// is optional; when omitted, errors are ignored (they still surface via
  /// `ingest`/`flush`/`wait_for_offset()`). Same lifetime rules as any
  /// `AckCallback`: whatever the handlers capture must outlive the callback.
  static std::shared_ptr<AckCallback> from(
      std::function<void(std::int64_t)> on_ack,
      std::function<void(std::int64_t, const std::string&)> on_error = nullptr);
};

}  // namespace zerobus

#endif  // ZEROBUS_ACK_CALLBACK_HPP
