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

/// Test-only harness that drives the *real* callback handler task and the
/// *real* teardown sequence in isolation, without a live gRPC connection.
///
/// There is no in-process mock gRPC server, so a fully end-to-end "stream over
/// a socket" callback test isn't hermetically feasible. This harness is the
/// faithful hermetic substitute: it wires up the same `spawn_callback_handler_task`
/// and channel that `ZerobusStream::new` uses, so tests push genuine
/// `CallbackMessage`s through it and then reproduce `close()`'s teardown via
/// [`ZerobusStream::shutdown_callback_task`] — the identical timeout-then-abort
/// / wait-indefinitely logic. It lets the FFI crate exercise the ack callback
/// lifetime + teardown contract against production code paths.
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

    /// Enqueues an ack for delivery through the handler task. Returns `true` if
    /// it was queued, `false` if the task's receiver is already gone (e.g. after
    /// teardown).
    pub fn send_ack(&self, offset_id: crate::OffsetId) -> bool {
        self.sender.send(CallbackMessage::Ack(offset_id)).is_ok()
    }

    /// Enqueues an error for delivery through the handler task. Returns `true` if
    /// it was queued, `false` if the task's receiver is already gone.
    pub fn send_error(&self, offset_id: crate::OffsetId, message: &str) -> bool {
        self.sender
            .send(CallbackMessage::Error(offset_id, message.to_string()))
            .is_ok()
    }

    /// Whether the handler task's receiver has been dropped — true once the task
    /// has exited (e.g. after [`Self::teardown`]), so no further message can be
    /// dispatched to the callback.
    pub fn is_closed(&self) -> bool {
        self.sender.is_closed()
    }

    /// Reproduces `ZerobusStream::close()`'s callback teardown: cancels the
    /// token, then drains the handler task via the production
    /// [`ZerobusStream::shutdown_callback_task`] path (timeout-then-abort when
    /// `callback_max_wait_time_ms` is `Some`, wait-indefinitely when `None`).
    ///
    /// When this returns, the handler task has stopped invoking the callback,
    /// so the caller may safely release the callback's `user_data`.
    pub async fn teardown(&mut self, callback_max_wait_time_ms: Option<u64>) {
        self.cancellation_token.cancel();
        if let Some(task) = self.task.take() {
            ZerobusStream::shutdown_callback_task(task, callback_max_wait_time_ms).await;
        }
    }
}
