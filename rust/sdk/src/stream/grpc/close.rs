//! Stream teardown: closing the stream and shutting down its tasks.
//!
//! Transport-agnostic: `close` flushes via the ack path, flips the closed
//! flag, and cancels the supervisor and callback tasks. The IO tasks observe
//! the cancellation and unwind on their own.

use std::sync::atomic::Ordering;

use tokio::time::Duration;
use tracing::{debug, error, info, warn};

use super::ZerobusStream;
use crate::ZerobusResult;

/// Maximum time to wait for the supervisor task to finish during stream
/// teardown.
const SHUTDOWN_TIMEOUT_SECS: u64 = 1;

impl ZerobusStream {
    /// Returns whether the stream has been closed.
    pub fn is_closed(&self) -> bool {
        self.is_closed.load(Ordering::Relaxed)
    }

    /// Closes the stream gracefully after flushing all pending records.
    ///
    /// This method first calls `flush()` to ensure all pending records are acknowledged,
    /// then shuts down the stream and releases all resources. Always call this method
    /// when you're done with a stream to ensure data integrity.
    ///
    /// # Returns
    ///
    /// `Ok(())` if the stream was closed successfully after flushing all records.
    ///
    /// # Errors
    ///
    /// Returns any errors from the flush operation. If flush fails, some records
    /// may not have been acknowledged. Use `get_unacked_records()` to retrieve them.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use databricks_zerobus_ingest_sdk::*;
    /// # async fn example(mut stream: ZerobusStream) -> Result<(), ZerobusError> {
    /// // After ingesting records...
    /// stream.close().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn close(&mut self) -> ZerobusResult<()> {
        if self.is_closed.load(Ordering::Relaxed) {
            return Ok(());
        }
        if let Some(stream_id) = self.stream_id.as_deref() {
            info!(stream_id = %stream_id, "Closing stream");
        } else {
            error!("Stream ID is None during closing");
        }
        let flush_result = self.flush().await;
        self.is_closed.store(true, Ordering::Relaxed);
        self.shutdown_all_tasks_gracefully().await;
        flush_result
    }

    /// Gracefully shuts down the supervisor task.
    ///
    /// Signals cancellation and waits for the task to exit. If the timeout
    /// is provided and expires, forcefully aborts the task.
    async fn shutdown_all_tasks_gracefully(&mut self) {
        self.cancellation_token.cancel();

        // Shutdown supervisor task.
        match tokio::time::timeout(
            Duration::from_secs(SHUTDOWN_TIMEOUT_SECS),
            &mut self.supervisor_task,
        )
        .await
        {
            Ok(_) => {
                debug!("Supervisor task exited gracefully");
            }
            Err(_) => {
                warn!("Supervisor task did not exit within timeout, aborting");
                self.supervisor_task.abort();
            }
        }
        // Shutdown callback handler task, if there are any callbacks.
        if let Some(task) = self.callback_handler_task.take() {
            Self::shutdown_callback_task(task, self.options.callback_max_wait_time_ms).await;
        }
    }

    /// Drains the callback handler task during teardown. The caller must have
    /// already cancelled the `cancellation_token`. With `Some(ms)`, waits up to
    /// that long then aborts; with `None`, waits indefinitely.
    ///
    /// Split out so the teardown can be exercised in isolation by tests
    /// (`CallbackHandlerHarness`, `testing` feature).
    pub(super) async fn shutdown_callback_task(
        mut task: tokio::task::JoinHandle<()>,
        callback_max_wait_time_ms: Option<u64>,
    ) {
        if let Some(callback_max_wait_time_ms) = callback_max_wait_time_ms {
            match tokio::time::timeout(Duration::from_millis(callback_max_wait_time_ms), &mut task)
                .await
            {
                Ok(_) => {
                    debug!("Callback handler task exited gracefully");
                }
                Err(_) => {
                    debug!("Callback handler task did not exit within timeout, aborting");
                    task.abort();
                }
            }
        } else {
            debug!("Callback max wait time is not set, waiting indefinitely");
            let _ = (&mut task).await;
        }
    }

    // Signal the stream to stop accepting work and tear down its background
    // tasks. Unlike `close`, this only needs `&self` — it relies on the
    // cancellation token and `is_closed` flag, both of which are already
    // interior-mutable. The `JoinHandle`s aren't reaped here; that happens in
    // `close` or `Drop`.
    #[cfg(feature = "testing")]
    pub(crate) fn signal_shutdown(&self) {
        self.is_closed.store(true, Ordering::Relaxed);
        self.cancellation_token.cancel();
    }
}
