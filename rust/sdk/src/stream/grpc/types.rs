//! Internal types shared across the `stream` module.
//!
//! These are transport-agnostic: they describe ingestion requests, ack
//! plumbing, and callback messages — none of them reference gRPC types and
//! are expected to be reused as-is when other transports (e.g. Arrow Flight)
//! are unified onto the same core.

use std::collections::HashMap;
use std::sync::Arc;

use crate::landing_zone::{Countable, LandingZone};
use crate::{EncodedBatch, OffsetId, ZerobusResult};

#[derive(Debug, Clone)]
pub(super) struct IngestRequest {
    pub(super) payload: EncodedBatch,
    pub(super) offset_id: OffsetId,
}

impl Countable for IngestRequest {
    fn count(&self) -> usize {
        self.payload.get_record_count()
    }
}

/// Map of logical offset to oneshot sender used to send acknowledgments back to the client.
pub(super) type OneshotMap =
    HashMap<OffsetId, tokio::sync::oneshot::Sender<ZerobusResult<OffsetId>>>;

/// Landing zone for ingest records.
pub(super) type RecordLandingZone = Arc<LandingZone<Box<IngestRequest>>>;

/// Messages sent to the callback handler task.
#[derive(Debug, Clone)]
pub(super) enum CallbackMessage {
    /// Acknowledgment callback with logical offset ID.
    Ack(OffsetId),
    /// Error callback with logical offset ID and error message.
    Error(OffsetId, String),
}
