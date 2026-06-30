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
