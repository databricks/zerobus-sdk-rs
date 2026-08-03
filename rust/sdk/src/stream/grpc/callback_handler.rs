//! Callback dispatch task.
//!
//! Transport-agnostic: drains a channel of `CallbackMessage`s (ack / error)
//! and invokes the user-supplied `AckCallback`. Decoupled from the IO tasks
//! so callbacks never block the gRPC receive loop.

use std::sync::Arc;

use tokio_util::sync::CancellationToken;
use tracing::{debug, instrument, span, Level};

use super::types::CallbackMessage;
use super::ZerobusStream;
use crate::AckCallback;

impl ZerobusStream {
    /// Spawns a task that handles callback execution in a separate thread.
    /// This task receives callback messages via a channel and executes them
    /// without blocking the receiver task.
    #[instrument(level = "debug", skip_all)]
    pub(super) fn spawn_callback_handler_task(
        mut callback_rx: tokio::sync::mpsc::UnboundedReceiver<CallbackMessage>,
        ack_callback: Option<Arc<dyn AckCallback>>,
        cancellation_token: CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let span = span!(Level::DEBUG, "callback_handler");
            let _guard = span.enter();
            loop {
                tokio::select! {
                    biased;
                    message = callback_rx.recv() => {
                        match message {
                            Some(message) => {
                                match message {
                                    CallbackMessage::Ack(logical_offset) => {
                                        if let Some(ref callback) = ack_callback {
                                            callback.on_ack(logical_offset);
                                        }
                                    }
                                    CallbackMessage::Error(logical_offset, error_message) => {
                                        if let Some(ref callback) = ack_callback {
                                            callback.on_error(logical_offset, &error_message);
                                        }
                                    }
                                }
                            }
                            None => { // This happens when all senders are dropped.
                                debug!("Callback handler task shutting down");
                                return;
                            }
                        }
                    }
                    _ = cancellation_token.cancelled() => {
                        debug!("Callback handler task cancelled");
                        return;
                    }

                }
            }
        })
    }
}

/// Test-only harness driving the real callback handler task and teardown
/// without a gRPC connection. Wires up the same `spawn_callback_handler_task` +
/// channel as `ZerobusStream::new_stream`, and its `teardown()` runs the production
/// `ZerobusStream::shutdown_callback_task`, so tests exercise the ack callback
/// lifetime contract against real code.
#[cfg(feature = "testing")]
pub struct CallbackHandlerHarness {
    sender: tokio::sync::mpsc::UnboundedSender<CallbackMessage>,
    task: Option<tokio::task::JoinHandle<()>>,
    cancellation_token: CancellationToken,
}

#[cfg(feature = "testing")]
impl CallbackHandlerHarness {
    /// Spawns the real callback handler task around the given `AckCallback`.
    pub fn spawn(ack_callback: Arc<dyn AckCallback>) -> Self {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let cancellation_token = CancellationToken::new();
        let task = ZerobusStream::spawn_callback_handler_task(
            rx,
            Some(ack_callback),
            cancellation_token.clone(),
        );
        Self {
            sender: tx,
            task: Some(task),
            cancellation_token,
        }
    }

    /// Enqueue an ack. Returns `false` if the task's receiver is already gone.
    pub fn send_ack(&self, offset_id: crate::OffsetId) -> bool {
        self.sender.send(CallbackMessage::Ack(offset_id)).is_ok()
    }

    /// Enqueue an error. Returns `false` if the task's receiver is already gone.
    pub fn send_error(&self, offset_id: crate::OffsetId, message: &str) -> bool {
        self.sender
            .send(CallbackMessage::Error(offset_id, message.to_string()))
            .is_ok()
    }

    /// Whether the handler task has exited (e.g. after teardown), so no further
    /// callback can be dispatched. Named distinctly from
    /// [`ZerobusStream::is_closed`] (the stream's `is_closed` flag) since this
    /// reports the callback channel's sender state instead.
    pub fn is_task_gone(&self) -> bool {
        self.sender.is_closed()
    }

    /// Reproduces `close()`'s callback teardown: cancel the token, then run the
    /// production `ZerobusStream::shutdown_callback_task`. On return the task
    /// has stopped, so `user_data` is safe to release.
    pub async fn teardown(&mut self, callback_max_wait_time_ms: Option<u64>) {
        self.cancellation_token.cancel();
        if let Some(task) = self.task.take() {
            ZerobusStream::shutdown_callback_task(task, callback_max_wait_time_ms).await;
        }
    }
}
