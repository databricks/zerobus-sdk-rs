//! Public ingestion methods and their shared internals.
//!
//! Transport-agnostic: these methods write into the landing zone and
//! oneshot/ack plumbing. Record format dependencies (`EncodedBatch`,
//! `EncodedRecord`) flow through, but no gRPC types appear here — the
//! sender task is what eventually moves bytes onto the wire.

use std::future::Future;
use std::sync::atomic::Ordering;
use tracing::{debug, error, warn};

use super::types::IngestRequest;
use super::ZerobusStream;
use crate::{EncodedBatch, EncodedRecord, OffsetId, ZerobusError, ZerobusResult};

impl ZerobusStream {
    /// Ingests a single record and returns its logical offset directly.
    ///
    /// Returns the logical offset after the record is queued. Use `wait_for_offset()`
    /// or `flush()` to wait for server acknowledgment.
    ///
    /// # Arguments
    ///
    /// * `payload` - A record that can be converted to `EncodedRecord` (either JSON string or protobuf bytes)
    ///
    /// # Returns
    ///
    /// The logical offset ID assigned to this record.
    ///
    /// # Errors
    ///
    /// * `InvalidArgument` - If the record type doesn't match stream configuration
    /// * `StreamClosedError` - If the stream has been closed
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use databricks_zerobus_ingest_sdk::*;
    /// # use prost::Message;
    /// # async fn example(stream: ZerobusStream) -> Result<(), ZerobusError> {
    /// # let my_record = vec![1, 2, 3]; // Example protobuf-encoded data
    /// // Ingest and get offset immediately
    /// let offset = stream.ingest_record_offset(my_record).await?;
    ///
    /// // Later, wait for acknowledgment
    /// stream.wait_for_offset(offset).await?;
    /// println!("Record at offset {} has been acknowledged", offset);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn ingest_record_offset(
        &self,
        payload: impl Into<EncodedRecord>,
    ) -> ZerobusResult<OffsetId> {
        let encoded_batch = self.prepare_record(payload)?;
        self.enqueue_prepared_batch(encoded_batch).await
    }

    /// Ingests a batch of records and returns the logical offset directly.
    ///
    /// Returns the logical offset after the batch is queued. Use `wait_for_offset()`
    /// or `flush()` to wait for server acknowledgment.
    ///
    /// # Arguments
    ///
    /// * `payload` - An iterator of records (each item should be convertible to `EncodedRecord`)
    ///
    /// # Returns
    ///
    /// `Some(offset_id)` for non-empty batches, or `None` if the batch is empty.
    ///
    /// # Errors
    ///
    /// * `InvalidArgument` - If record types don't match stream configuration
    /// * `StreamClosedError` - If the stream has been closed
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use databricks_zerobus_ingest_sdk::*;
    /// # use prost::Message;
    /// # async fn example(stream: ZerobusStream) -> Result<(), ZerobusError> {
    /// let records = vec![vec![1, 2, 3], vec![4, 5, 6]]; // Example protobuf-encoded data
    ///
    /// // Ingest batch and get offset immediately
    /// if let Some(offset) = stream.ingest_records_offset(records).await? {
    ///     // Later, wait for batch acknowledgment
    ///     stream.wait_for_offset(offset).await?;
    ///     println!("Batch at offset {} has been acknowledged", offset);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn ingest_records_offset<I, T>(&self, payload: I) -> ZerobusResult<Option<OffsetId>>
    where
        I: IntoIterator<Item = T>,
        T: Into<EncodedRecord>,
    {
        let encoded_batch = self.prepare_records(payload)?;

        if encoded_batch.is_empty() {
            Ok(None)
        } else {
            self.enqueue_prepared_batch(encoded_batch)
                .await
                .map(Option::Some)
        }
    }

    #[allow(clippy::result_large_err)]
    pub(crate) fn prepare_record(
        &self,
        payload: impl Into<EncodedRecord>,
    ) -> ZerobusResult<EncodedBatch> {
        let encoded_batch = EncodedBatch::try_from_record(payload, self.options.record_type)
            .ok_or_else(|| {
                ZerobusError::InvalidArgument(
                    "Record type does not match stream configuration".to_string(),
                )
            })?;
        self.validate_ingest_payload(&encoded_batch)?;
        Ok(encoded_batch)
    }

    #[allow(clippy::result_large_err)]
    pub(crate) fn prepare_records<I, T>(&self, payload: I) -> ZerobusResult<EncodedBatch>
    where
        I: IntoIterator<Item = T>,
        T: Into<EncodedRecord>,
    {
        let encoded_batch = EncodedBatch::try_from_batch(payload, self.options.record_type)
            .ok_or_else(|| {
                ZerobusError::InvalidArgument(
                    "Record type does not match stream configuration".to_string(),
                )
            })?;
        self.validate_ingest_payload(&encoded_batch)?;
        Ok(encoded_batch)
    }

    #[allow(clippy::result_large_err)]
    fn validate_ingest_payload(&self, encoded_batch: &EncodedBatch) -> ZerobusResult<()> {
        let byte_size = encoded_batch.total_byte_size();
        let max_payload_bytes = self.options.max_ingest_payload_bytes;
        if byte_size > max_payload_bytes {
            return Err(ZerobusError::InvalidArgument(format!(
                "Ingest payload too large: {byte_size} bytes exceeds the configured limit of {max_payload_bytes} bytes"
            )));
        }
        Ok(())
    }

    #[allow(clippy::result_large_err)]
    pub(crate) fn check_open(&self) -> ZerobusResult<()> {
        if self.is_closed.load(Ordering::Relaxed) {
            error!(table_name = %self.table_properties.table_name, "Stream closed");
            return Err(ZerobusError::StreamClosedError(tonic::Status::internal(
                "Stream closed",
            )));
        }
        Ok(())
    }

    /// Internal unified method for ingesting records and batches.
    ///
    /// Returns a future that resolves once the server has acknowledged the batch.
    /// Used by `ZerobusSdk::recreate_stream` to replay unacknowledged batches.
    pub(crate) async fn ingest_internal(
        &self,
        encoded_batch: EncodedBatch,
    ) -> ZerobusResult<impl Future<Output = ZerobusResult<OffsetId>>> {
        let reservation = self.reserve_capacity().await?;

        let _guard = self.sync_mutex.lock().await;
        self.check_open()?;

        let offset_id = self.logical_offset_id_generator.next();
        debug!(
            offset_id = offset_id,
            record_count = encoded_batch.get_record_count(),
            "Ingesting record(s)"
        );

        if let Some(stream_id) = self.stream_id.as_ref() {
            let (tx, rx) = tokio::sync::oneshot::channel();
            {
                let mut map = self.oneshot_map.lock().await;
                map.insert(offset_id, tx);
            }
            self.landing_zone.enqueue_reserved(
                Box::new(IngestRequest {
                    payload: encoded_batch,
                    offset_id,
                }),
                reservation,
            );
            let stream_id = stream_id.to_string();
            Ok(async move {
                rx.await.map_err(|err| {
                    error!(stream_id = %stream_id, "Failed to receive ack: {}", err);
                    ZerobusError::StreamClosedError(tonic::Status::internal(
                        "Failed to receive ack",
                    ))
                })?
            })
        } else {
            error!("Stream ID is None");
            Err(ZerobusError::StreamClosedError(tonic::Status::internal(
                "Stream ID is None",
            )))
        }
    }

    /// Internal unified method for ingesting records and batches.
    ///
    /// Returns the logical offset directly without waiting for acknowledgment.
    /// Used by the public `ingest_*_offset` methods.
    async fn enqueue_prepared_batch(&self, encoded_batch: EncodedBatch) -> ZerobusResult<OffsetId> {
        let reservation = self.reserve_capacity().await?;

        let _guard = self.sync_mutex.lock().await;
        self.check_open()?;

        let offset_id = self.logical_offset_id_generator.next();
        debug!(
            offset_id = offset_id,
            record_count = encoded_batch.get_record_count(),
            "Ingesting record(s)"
        );
        self.landing_zone.enqueue_reserved(
            Box::new(IngestRequest {
                payload: encoded_batch,
                offset_id,
            }),
            reservation,
        );
        Ok(offset_id)
    }

    pub(crate) async fn reserve_capacity(
        &self,
    ) -> ZerobusResult<crate::landing_zone::CapacityReservation> {
        self.check_open()?;
        let started_at = tokio::time::Instant::now();
        let table_name = self.table_properties.table_name.as_str();
        let max_inflight_requests = self.options.max_inflight_requests;
        tokio::select! {
            reservation = self.landing_zone.reserve_capacity() => Ok(reservation),
            _ = self.terminal_token.cancelled() => {
                let waited_ms = started_at.elapsed().as_millis();
                let cause = self
                    .server_error_rx
                    .borrow()
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_else(|| "stream closed without a recorded server error".to_string());
                warn!(
                    table_name,
                    waited_ms,
                    max_inflight_requests,
                    cause,
                    "Stream capacity wait cancelled by terminal shutdown"
                );
                Err(ZerobusError::StreamClosedError(tonic::Status::internal(
                    format!(
                        "Stream for table {table_name} terminated after {waited_ms} ms while waiting for capacity (max_inflight_requests: {max_inflight_requests}; cause: {cause})"
                    ),
                )))
            }
        }
    }

    #[cfg(feature = "testing")]
    pub(crate) async fn enqueue_reserved_admitted<F, Fut, G>(
        &self,
        encoded_batch: EncodedBatch,
        reservation: crate::landing_zone::CapacityReservation,
        admit: F,
    ) -> ZerobusResult<OffsetId>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = ZerobusResult<G>>,
    {
        let _guard = self.sync_mutex.lock().await;
        let admission_guard = admit().await?;
        self.check_open()?;

        let offset_id = self.logical_offset_id_generator.next();
        debug!(
            offset_id,
            record_count = encoded_batch.get_record_count(),
            "Ingesting record(s)"
        );
        self.landing_zone.enqueue_reserved(
            Box::new(IngestRequest {
                payload: encoded_batch,
                offset_id,
            }),
            reservation,
        );
        drop(admission_guard);
        Ok(offset_id)
    }
}
