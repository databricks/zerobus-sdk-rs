//! Acknowledgment tracking: waiting for offsets and inspecting unacked records.
//!
//! Transport-agnostic: these only touch the offset generator, watch channels,
//! and the failed-records buffer. They don't move bytes; they observe progress
//! the IO tasks report back.

use std::sync::atomic::Ordering;

use tokio::time::Duration;
use tracing::{debug, error, instrument, trace};

use super::ZerobusStream;
use crate::{EncodedBatch, EncodedRecord, OffsetId, ZerobusError, ZerobusResult};

impl ZerobusStream {
    /// Internal method to wait for a specific offset to be acknowledged.
    /// Used by both `flush()` and `wait_for_offset()`.
    async fn wait_for_offset_internal(
        &self,
        offset_to_wait: OffsetId,
        operation_name: &str,
    ) -> ZerobusResult<()> {
        let wait_operation = async {
            let mut offset_receiver = self.logical_last_received_offset_id_tx.subscribe();
            let mut error_rx = self.server_error_rx.clone();

            loop {
                let offset = *offset_receiver.borrow_and_update();

                let stream_id = match self.stream_id.as_deref() {
                    Some(stream_id) => stream_id,
                    None => {
                        error!("Stream ID is None during {}", operation_name.to_lowercase());
                        "None"
                    }
                };
                if let Some(offset) = offset {
                    if offset >= offset_to_wait {
                        debug!(stream_id = %stream_id, "Stream is caught up to the given offset. {} completed.", operation_name);
                        return Ok(());
                    } else {
                        trace!(
                            stream_id = %stream_id,
                            "Stream is caught up to offset {}. Waiting for offset {}.",
                            offset, offset_to_wait
                        );
                    }
                } else {
                    trace!(
                        stream_id = %stream_id,
                        "Stream is not caught up to any offset yet. Waiting for the first offset."
                    );
                }
                if self.is_closed.load(Ordering::Relaxed) {
                    // Re-check offset before failing, it might have been updated.
                    let offset = *offset_receiver.borrow_and_update();
                    if let Some(offset) = offset {
                        if offset >= offset_to_wait {
                            return Ok(());
                        }
                    }
                    // The supervisor always sends the real error to server_error_tx
                    // before setting is_closed=true, so check error_rx first to
                    // return the actual error instead of a generic one.
                    if let Some(server_error) = error_rx.borrow().clone() {
                        return Err(server_error);
                    }
                    return Err(ZerobusError::StreamClosedError(tonic::Status::internal(
                        format!("Stream closed during {}", operation_name.to_lowercase()),
                    )));
                }
                // Race between offset updates and server errors.
                tokio::select! {
                    result = offset_receiver.changed() => {
                        // If offset_receiver channel is closed, break the loop.
                        if result.is_err() {
                            break;
                        }
                        // Loop continues to check new offset value.
                    }
                    _ = error_rx.changed() => {
                        // Server error occurred, return it immediately if stream is closed.
                        if let Some(server_error) = error_rx.borrow().clone() {
                            if self.is_closed.load(Ordering::Relaxed) {
                                // Re-check offset before failing, it might have been updated.
                                let offset = *offset_receiver.borrow_and_update();
                                if let Some(offset) = offset {
                                    if offset >= offset_to_wait {
                                        return Ok(());
                                    }
                                }
                                return Err(server_error);
                            }
                        }
                    }
                }
            }

            if let Some(server_error) = error_rx.borrow().clone() {
                if self.is_closed.load(Ordering::Relaxed) {
                    return Err(server_error);
                }
            }

            Err(ZerobusError::StreamClosedError(tonic::Status::internal(
                format!("Stream closed during {}", operation_name.to_lowercase()),
            )))
        };

        match tokio::time::timeout(
            Duration::from_millis(self.options.flush_timeout_ms),
            wait_operation,
        )
        .await
        {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(e),
            Err(_) => {
                if let Some(stream_id) = self.stream_id.as_deref() {
                    error!(stream_id = %stream_id, table_name = %self.table_properties.table_name, "{} timed out", operation_name);
                } else {
                    error!(table_name = %self.table_properties.table_name, "{} timed out", operation_name);
                }
                Err(ZerobusError::StreamClosedError(
                    tonic::Status::deadline_exceeded(format!("{} timed out", operation_name)),
                ))
            }
        }
    }

    /// Flushes all currently pending records and waits for their acknowledgments.
    ///
    /// This method captures the current highest offset and waits until all records up to
    /// that offset have been acknowledged by the server. Records ingested during the flush
    /// operation are not included in this flush.
    ///
    /// # Returns
    ///
    /// `Ok(())` when all pending records at the time of the call have been acknowledged.
    ///
    /// # Errors
    ///
    /// * `StreamClosedError` - If the stream is closed or times out
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use databricks_zerobus_ingest_sdk::*;
    /// # async fn example(stream: ZerobusStream) -> Result<(), ZerobusError> {
    /// // Ingest many records
    /// for i in 0..1000 {
    ///     let _offset = stream.ingest_record_offset(vec![i as u8]).await?;
    /// }
    ///
    /// // Wait for all to be acknowledged
    /// stream.flush().await?;
    /// println!("All 1000 records have been acknowledged");
    /// # Ok(())
    /// # }
    /// ```
    #[instrument(level = "debug", skip_all, fields(table_name = %self.table_properties.table_name))]
    pub async fn flush(&self) -> ZerobusResult<()> {
        let offset_to_wait = match self.logical_offset_id_generator.last() {
            Some(offset) => offset,
            None => return Ok(()), // Nothing to flush.
        };
        self.wait_for_offset_internal(offset_to_wait, "Flush").await
    }

    /// Waits for server acknowledgment of a specific logical offset.
    ///
    /// This method blocks until the server has acknowledged the record or batch at the
    /// specified offset. Use this with offsets returned from `ingest_record_offset()` or
    /// `ingest_records_offset()` to explicitly control when to wait for acknowledgments.
    ///
    /// # Arguments
    ///
    /// * `offset` - The logical offset ID to wait for (returned from `ingest_record_offset()` or `ingest_records_offset()`)
    ///
    /// # Returns
    ///
    /// `Ok(())` when the record/batch at the specified offset has been acknowledged.
    ///
    /// # Errors
    ///
    /// * `StreamClosedError` - If the stream is closed or times out while waiting
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use databricks_zerobus_ingest_sdk::*;
    /// # async fn example(stream: ZerobusStream) -> Result<(), ZerobusError> {
    /// # let my_record = vec![1, 2, 3];
    /// // Ingest multiple records and collect their offsets
    /// let mut offsets = Vec::new();
    /// for i in 0..100 {
    ///     let offset = stream.ingest_record_offset(vec![i as u8]).await?;
    ///     offsets.push(offset);
    /// }
    ///
    /// // Wait for specific offsets
    /// for offset in offsets {
    ///     stream.wait_for_offset(offset).await?;
    /// }
    /// println!("All records acknowledged");
    /// # Ok(())
    /// # }
    /// ```
    pub async fn wait_for_offset(&self, offset: OffsetId) -> ZerobusResult<()> {
        self.wait_for_offset_internal(offset, "Waiting for acknowledgement")
            .await
    }

    /// Returns all records that were ingested but not acknowledged by the server.
    ///
    /// This method should only be called after a stream has failed or been closed.
    /// It's useful for implementing custom retry logic or persisting failed records.
    ///
    /// **Note:** This method flattens all unacknowledged records into a single iterator,
    /// losing the original batch grouping.
    /// If you want to preserve the batch grouping, use `ZerobusStream::get_unacked_batches()` instead.
    /// If you want to re-ingest unacknowledged records while preserving their batch
    /// structure, use `ZerobusSdk::recreate_stream()` instead.
    ///
    ///
    /// # Returns
    ///
    /// An iterator over individual `EncodedRecord` items. All unacknowledged records are
    /// flattened into a single sequence, regardless of how they were originally ingested
    /// (via `ingest_record()` or `ingest_records()`).
    ///
    /// # Errors
    ///
    /// * `InvalidStateError` - If called on an active (not closed) stream
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use databricks_zerobus_ingest_sdk::*;
    /// # async fn example(sdk: ZerobusSdk, mut stream: ZerobusStream) -> Result<(), ZerobusError> {
    /// match stream.close().await {
    ///     Err(e) => {
    ///         // Stream failed, get unacked records
    ///         let unacked = stream.get_unacked_records().await?;
    ///         let total_records = unacked.into_iter().count();
    ///         println!("Failed to acknowledge {} records", total_records);
    ///
    ///         // For re-ingestion with preserved batch structure, use recreate_stream
    ///         let new_stream = sdk.recreate_stream(&stream).await?;
    ///     }
    ///     Ok(_) => println!("All records acknowledged"),
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_unacked_records(&self) -> ZerobusResult<impl Iterator<Item = EncodedRecord>> {
        Ok(self
            .get_unacked_batches()
            .await?
            .into_iter()
            .flat_map(|batch| batch.into_iter()))
    }

    /// Returns all records that were ingested but not acknowledged by the server, grouped by batch.
    ///
    /// This method should only be called after a stream has failed or been closed.
    /// It's useful for implementing custom retry logic or persisting failed records.
    ///
    /// **Note:** This method returns the unacknowledged records as a vector of `EncodedBatch` items,
    /// where each batch corresponds to how records were ingested:
    /// - Each `ingest_record()` call creates a single batch containing one record
    /// - Each `ingest_records()` call creates a single batch containing multiple records
    ///
    /// For alternatives, see `ZerobusStream::get_unacked_records()` and `ZerobusSdk::recreate_stream()`.
    ///
    /// # Returns
    ///
    /// A vector of `EncodedBatch` items. Records are grouped by their original ingestion call.
    pub async fn get_unacked_batches(&self) -> ZerobusResult<Vec<EncodedBatch>> {
        if self.is_closed.load(Ordering::Relaxed) {
            // The supervisor only moves landing-zone records into
            // `failed_records` on a stream failure. A stream torn down without
            // one (flush timed out during `close`, or `signal_shutdown` from a
            // poisoned MultiplexedStream) can still hold unacked records in the
            // landing zone; drain them here so they are reported too and so
            // repeat calls return the same result.
            let mut failed = self.failed_records.write().await;
            failed.extend(
                self.landing_zone
                    .remove_all()
                    .into_iter()
                    .map(|request| request.payload),
            );
            return Ok(failed.clone());
        }
        if let Some(stream_id) = self.stream_id.as_deref() {
            error!(stream_id = %stream_id, "Cannot get unacked records from an active stream. Stream must be closed first.");
        } else {
            error!(
                "Cannot get unacked records from an active stream. Stream must be closed first."
            );
        }
        Err(ZerobusError::InvalidStateError(
            "Cannot get unacked records from an active stream. Stream must be closed first."
                .to_string(),
        ))
    }
}
