//! Public ingestion methods and their shared internals.
//!
//! Transport-agnostic: these methods write into the landing zone and
//! oneshot/ack plumbing. Record format dependencies (`EncodedBatch`,
//! `EncodedRecord`) flow through, but no gRPC types appear here — the
//! sender task is what eventually moves bytes onto the wire.

use std::future::Future;
use std::sync::atomic::Ordering;
use tracing::{debug, error};

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
        let encoded_batch = EncodedBatch::try_from_record(payload, self.options.record_type)
            .ok_or_else(|| {
                ZerobusError::InvalidArgument(
                    "Record type does not match stream configuration".to_string(),
                )
            })?;

        self.ingest_internal_v2(encoded_batch).await
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
        let encoded_batch = EncodedBatch::try_from_batch(payload, self.options.record_type)
            .ok_or_else(|| {
                ZerobusError::InvalidArgument(
                    "Record type does not match stream configuration".to_string(),
                )
            })?;

        if encoded_batch.is_empty() {
            Ok(None)
        } else {
            self.ingest_internal_v2(encoded_batch)
                .await
                .map(Option::Some)
        }
    }

    /// Internal unified method for ingesting records and batches.
    ///
    /// Returns a future that resolves once the server has acknowledged the batch.
    /// Used by `ZerobusSdk::recreate_stream` to replay unacknowledged batches.
    pub(crate) async fn ingest_internal(
        &self,
        encoded_batch: EncodedBatch,
    ) -> ZerobusResult<impl Future<Output = ZerobusResult<OffsetId>>> {
        if self.is_closed.load(Ordering::Relaxed) {
            error!(table_name = %self.table_properties.table_name, "Stream closed");
            return Err(ZerobusError::StreamClosedError(tonic::Status::internal(
                "Stream closed",
            )));
        }

        let _guard = self.sync_mutex.lock().await;

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
            self.landing_zone
                .add(Box::new(IngestRequest {
                    payload: encoded_batch,
                    offset_id,
                }))
                .await;
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
    async fn ingest_internal_v2(&self, encoded_batch: EncodedBatch) -> ZerobusResult<OffsetId> {
        let byte_size = encoded_batch.total_byte_size();
        let max_payload_bytes = self.options.max_ingest_payload_bytes;
        if byte_size > max_payload_bytes {
            return Err(ZerobusError::InvalidArgument(format!(
                "Ingest payload too large: {byte_size} bytes exceeds the configured limit of {max_payload_bytes} bytes"
            )));
        }

        if self.is_closed.load(Ordering::Relaxed) {
            error!(table_name = %self.table_properties.table_name, "Stream closed");
            return Err(ZerobusError::StreamClosedError(tonic::Status::internal(
                "Stream closed",
            )));
        }

        let _guard = self.sync_mutex.lock().await;

        let offset_id = self.logical_offset_id_generator.next();
        debug!(
            offset_id = offset_id,
            record_count = encoded_batch.get_record_count(),
            "Ingesting record(s)"
        );
        self.landing_zone
            .add(Box::new(IngestRequest {
                payload: encoded_batch,
                offset_id,
            }))
            .await;
        Ok(offset_id)
    }

    #[cfg(feature = "testing")]
    pub(crate) fn has_capacity(&self) -> bool {
        self.landing_zone.len() < self.options.max_inflight_requests
    }
}
