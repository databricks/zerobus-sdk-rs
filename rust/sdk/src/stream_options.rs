//! Shared configuration options for stream creation and operation.
//!
//! This module provides common configuration constants shared between gRPC and Arrow Flight streams.

use std::time::Duration;

use tokio_retry::strategy::{jitter, ExponentialBackoff, FixedInterval};

/// Retry strategy for stream creation and recovery.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum RetryStrategy {
    /// Retry after the configured `recovery_backoff_ms` interval.
    Fixed,
    /// Retry with exponential backoff and jitter, starting from `recovery_backoff_ms`.
    #[default]
    ExponentialBackoffWithJitter,
}

const EXPONENTIAL_BACKOFF_BASE: u64 = 2;

pub(crate) fn recovery_retry_strategy(
    retry_strategy: RetryStrategy,
    recovery_backoff_ms: u64,
    max_recovery_backoff_ms: u64,
) -> Box<dyn Iterator<Item = Duration> + Send> {
    match retry_strategy {
        RetryStrategy::Fixed => Box::new(FixedInterval::from_millis(recovery_backoff_ms)),
        RetryStrategy::ExponentialBackoffWithJitter => Box::new(
            ExponentialBackoff::from_millis(EXPONENTIAL_BACKOFF_BASE)
                .map(move |duration| {
                    capped_exponential_delay(duration, recovery_backoff_ms, max_recovery_backoff_ms)
                })
                .map(jitter),
        ),
    }
}

fn capped_exponential_delay(
    exponential_delay: Duration,
    recovery_backoff_ms: u64,
    max_recovery_backoff_ms: u64,
) -> Duration {
    let multiplier = exponential_delay.as_millis() / u128::from(EXPONENTIAL_BACKOFF_BASE);
    let millis = u128::from(recovery_backoff_ms)
        .saturating_mul(multiplier)
        .min(u128::from(max_recovery_backoff_ms))
        .min(u128::from(u64::MAX));

    Duration::from_millis(millis as u64)
}

/// Default values for stream configuration options.
/// These are shared between gRPC and Arrow Flight streams.
pub mod defaults {
    /// Default: enable automatic stream recovery
    pub const RECOVERY: bool = true;
    /// Default: 15 seconds per recovery attempt
    pub const RECOVERY_TIMEOUT_MS: u64 = 15_000;
    /// Default: 2 seconds initial backoff between retries
    pub const RECOVERY_BACKOFF_MS: u64 = 2_000;
    /// Default: cap exponential recovery backoff at 30 seconds
    pub const MAX_RECOVERY_BACKOFF_MS: u64 = 30_000;
    /// Default: 4 retry attempts
    pub const RECOVERY_RETRIES: u32 = 4;
    /// Default: 60 seconds lack of ack timeout
    pub const SERVER_LACK_OF_ACK_TIMEOUT_MS: u64 = 60_000;
    /// Default: 5 minutes flush timeout
    pub const FLUSH_TIMEOUT_MS: u64 = 300_000;
    /// Default: 30 seconds connection timeout
    #[cfg(feature = "arrow-flight")]
    pub const CONNECTION_TIMEOUT_MS: u64 = 30_000;
    /// Default: 5 seconds callback timeout
    pub const CALLBACK_MAX_WAIT_TIME_MS: u64 = 5_000;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_strategy_defaults_to_exponential_with_jitter() {
        assert_eq!(
            RetryStrategy::default(),
            RetryStrategy::ExponentialBackoffWithJitter
        );
        assert_eq!(defaults::MAX_RECOVERY_BACKOFF_MS, 30_000);
    }

    #[test]
    fn fixed_retry_strategy_uses_configured_interval() {
        let mut strategy = recovery_retry_strategy(RetryStrategy::Fixed, 123, 30_000);

        assert_eq!(strategy.next(), Some(Duration::from_millis(123)));
        assert_eq!(strategy.next(), Some(Duration::from_millis(123)));
    }

    #[test]
    fn exponential_retry_strategy_starts_from_initial_delay_and_caps() {
        assert_eq!(
            capped_exponential_delay(Duration::from_millis(2), 100, 1_000),
            Duration::from_millis(100)
        );
        assert_eq!(
            capped_exponential_delay(Duration::from_millis(4), 100, 1_000),
            Duration::from_millis(200)
        );
        assert_eq!(
            capped_exponential_delay(Duration::from_millis(16), 100, 250),
            Duration::from_millis(250)
        );
    }

    #[test]
    fn exponential_retry_strategy_applies_jitter_under_cap() {
        let mut strategy =
            recovery_retry_strategy(RetryStrategy::ExponentialBackoffWithJitter, 100, 250);

        for _ in 0..10 {
            assert!(strategy.next().unwrap() <= Duration::from_millis(250));
        }
    }
}
