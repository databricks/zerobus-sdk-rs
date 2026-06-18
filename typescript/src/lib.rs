// Zerobus TypeScript SDK - NAPI-RS Bindings
//
// This file provides the Node.js/TypeScript bindings for the Rust Zerobus SDK
// using NAPI-RS. It exposes a TypeScript-friendly API while leveraging the
// high-performance Rust implementation underneath.
//
// The binding layer handles:
// - Type conversions between JavaScript and Rust types
// - Async/await bridging (Rust futures → JavaScript Promises)
// - Memory management and thread safety
// - Error propagation

#![deny(clippy::all)]

use napi::bindgen_prelude::*;
use napi::threadsafe_function::{ErrorStrategy, ThreadsafeFunction};
use napi::{Env, JsFunction, JsGlobal, JsObject, JsString, JsUnknown, ValueType};
use napi_derive::napi;

use async_trait::async_trait;
use databricks_zerobus_ingest_sdk::databricks::zerobus::RecordType as RustRecordType;
use databricks_zerobus_ingest_sdk::{
    DefaultTokenFactory, EncodedRecord as RustRecordPayload,
    HeadersProvider as RustHeadersProvider, ZerobusError as RustZerobusError,
    ZerobusResult as RustZerobusResult, ZerobusSdk as RustZerobusSdk,
    ZerobusStream as RustZerobusStream,
};
use prost_types;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

/// User-Agent header value for TypeScript SDK requests.
const TS_SDK_USER_AGENT: &str = concat!("zerobus-sdk-ts/", env!("CARGO_PKG_VERSION"));

/// Record serialization format.
///
/// Specifies how records should be encoded when ingested into the stream.
#[napi]
pub enum RecordType {
    /// JSON encoding - records are JSON-encoded strings
    Json = 0,
    /// Protocol Buffers encoding - records are binary protobuf messages
    Proto = 1,
}

/// Configuration options for the Zerobus stream.
///
/// These options control stream behavior including recovery, timeouts, and inflight limits.
#[napi(object)]
pub struct StreamConfigurationOptions {
    /// Maximum number of unacknowledged requests that can be in flight.
    /// Default: 10,000
    pub max_inflight_requests: Option<u32>,

    /// Enable automatic stream recovery on transient failures.
    /// Default: true
    pub recovery: Option<bool>,

    /// Timeout for recovery operations in milliseconds.
    /// Default: 15,000 (15 seconds)
    pub recovery_timeout_ms: Option<u32>,

    /// Delay between recovery retry attempts in milliseconds.
    /// Default: 2,000 (2 seconds)
    pub recovery_backoff_ms: Option<u32>,

    /// Maximum number of recovery attempts before giving up.
    /// Default: 4
    pub recovery_retries: Option<u32>,

    /// Timeout for flush operations in milliseconds.
    /// Default: 300,000 (5 minutes)
    pub flush_timeout_ms: Option<u32>,

    /// Timeout waiting for server acknowledgments in milliseconds.
    /// Default: 60,000 (1 minute)
    pub server_lack_of_ack_timeout_ms: Option<u32>,

    /// Record serialization format.
    /// Use RecordType.Json for JSON encoding or RecordType.Proto for Protocol Buffers.
    /// Default: RecordType.Proto (Protocol Buffers)
    pub record_type: Option<i32>,

    /// Maximum wait time during graceful stream close in milliseconds.
    /// When the server signals stream closure, this controls how long to wait
    /// for in-flight records to be acknowledged.
    /// - None (undefined): Wait for full server-specified duration
    /// - Some(0): Immediately trigger recovery without waiting
    /// - Some(x): Wait up to min(x, server_duration) milliseconds
    /// Default: None (wait for full server duration)
    pub stream_paused_max_wait_time_ms: Option<u32>,
}

/// Properties of the target Delta table for ingestion.
///
/// Specifies which Unity Catalog table to write to and optionally the schema descriptor
/// for Protocol Buffers encoding.
#[napi(object)]
#[derive(Debug, Clone)]
pub struct TableProperties {
    /// Full table name in Unity Catalog (e.g., "catalog.schema.table")
    pub table_name: String,

    /// Optional Protocol Buffer descriptor as a base64-encoded string.
    /// If not provided, JSON encoding will be used.
    pub descriptor_proto: Option<String>,
}

/// Custom error type for Zerobus operations.
///
/// This error type includes information about whether the error is retryable,
/// which helps determine if automatic recovery can resolve the issue.
#[napi]
pub struct ZerobusError {
    message: String,
    is_retryable: bool,
}

#[napi]
impl ZerobusError {
    /// Returns true if this error can be automatically retried by the SDK.
    #[napi(getter)]
    pub fn is_retryable(&self) -> bool {
        self.is_retryable
    }

    /// Get the error message.
    #[napi(getter)]
    pub fn message(&self) -> String {
        self.message.clone()
    }
}

/// Convert a JS `BigInt` to `i64`, erroring if it can't be represented losslessly.
/// Used by `waitForOffset` to avoid the precision loss of the old
/// `Number(bigint)` round-trip past 2^53.
fn bigint_to_i64(value: BigInt) -> Result<i64> {
    let (n, lossless) = value.get_i64();
    if !lossless {
        return Err(Error::from_reason(
            "offsetId exceeds i64 range; cannot be represented without loss",
        ));
    }
    Ok(n)
}

/// Helper function to convert a JavaScript value to a RustRecordPayload.
///
/// Supports:
/// - Buffer (low-level proto bytes)
/// - string (low-level JSON string)
/// - Protobuf message object with .encode() method (high-level, auto-serializes)
/// - Plain JavaScript object (high-level, auto-stringifies to JSON)
fn convert_js_to_record_payload(env: &Env, payload: Unknown) -> Result<RustRecordPayload> {
    let value_type = payload.get_type()?;

    match value_type {
        ValueType::Object => {
            let js_value: JsUnknown = payload.try_into()?;
            if js_value.is_buffer()? {
                let buffer: Buffer = Buffer::from_unknown(js_value)?;
                return Ok(RustRecordPayload::Proto(buffer.to_vec()));
            }

            let obj: JsObject = JsObject::from_unknown(js_value)?;

            let constructor: JsFunction = obj.get_named_property("constructor")?;
            let constructor_obj = JsObject::from_unknown(constructor.into_unknown())?;

            if constructor_obj.has_named_property("encode")? {
                let encode_fn: JsFunction = constructor_obj.get_named_property("encode")?;
                let obj_as_unknown = obj.into_unknown();
                let encode_result: JsUnknown =
                    encode_fn.call::<JsUnknown>(Some(&constructor_obj), &[obj_as_unknown])?;
                let encode_obj = JsObject::from_unknown(encode_result)?;

                if encode_obj.has_named_property("finish")? {
                    let finish_fn: JsFunction = encode_obj.get_named_property("finish")?;
                    let buffer_result: JsUnknown =
                        finish_fn.call::<JsUnknown>(Some(&encode_obj), &[])?;
                    let buffer: Buffer = Buffer::from_unknown(buffer_result)?;
                    Ok(RustRecordPayload::Proto(buffer.to_vec()))
                } else {
                    Err(Error::from_reason(
                        "Protobuf message .encode() must return an object with .finish() method",
                    ))
                }
            } else {
                let global: JsGlobal = env.get_global()?;
                let json_obj: JsObject = global.get_named_property("JSON")?;
                let stringify: JsFunction = json_obj.get_named_property("stringify")?;
                let obj_as_unknown = obj.into_unknown();
                let str_result: JsUnknown =
                    stringify.call::<JsUnknown>(Some(&json_obj), &[obj_as_unknown])?;
                let js_string = JsString::from_unknown(str_result)?;
                let json_string = js_string.into_utf8()?.as_str()?.to_string();

                Ok(RustRecordPayload::Json(json_string))
            }
        }
        ValueType::String => {
            let js_value: JsUnknown = payload.try_into()?;
            let js_string = JsString::from_unknown(js_value)?;
            let json_string = js_string.into_utf8()?.as_str()?.to_string();
            Ok(RustRecordPayload::Json(json_string))
        }
        _ => Err(Error::from_reason(
            "Payload must be a Buffer, string, protobuf message object, or plain JavaScript object",
        )),
    }
}

/// A stream for ingesting data into a Databricks Delta table.
///
/// The stream manages a bidirectional gRPC connection, handles acknowledgments,
/// and provides automatic recovery on transient failures.
///
/// # Example
///
/// ```typescript
/// const stream = await sdk.createStream(tableProps, clientId, clientSecret, options);
/// const ackPromise = await stream.ingestRecord(Buffer.from([1, 2, 3]));
/// const offset = await ackPromise;
/// await stream.close();
/// ```
#[napi]
pub struct ZerobusStream {
    inner: Arc<Mutex<Option<RustZerobusStream>>>,
}

#[napi]
impl ZerobusStream {
    /// Ingests a single record into the stream.
    ///
    /// **@deprecated** Use `ingestRecordOffset()` instead, which returns the offset directly
    /// after queuing. Then use `waitForOffset()` to wait for acknowledgment when needed.
    ///
    /// This method accepts either:
    /// - A Protocol Buffer encoded record as a Buffer (Vec<u8>)
    /// - A JSON string
    ///
    /// This method BLOCKS until the record is sent to the SDK's internal landing zone,
    /// then returns a Promise for the server acknowledgment. This allows you to send
    /// many records immediately without waiting for acknowledgments:
    ///
    /// ```typescript
    /// let lastAckPromise;
    /// for (let i = 0; i < 1000; i++) {
    ///     // This call blocks until record is sent (in SDK)
    ///     lastAckPromise = stream.ingestRecord(record);
    /// }
    /// // All 1000 records are now in the SDK's internal queue
    /// // Wait for the last acknowledgment
    /// await lastAckPromise;
    /// // Flush to ensure all records are acknowledged
    /// await stream.flush();
    /// ```
    ///
    /// # Arguments
    ///
    /// * `payload` - The record data. Accepts:
    ///   - Buffer (low-level proto bytes)
    ///   - string (low-level JSON string)
    ///   - Protobuf message object with .encode() method (high-level, auto-serializes)
    ///   - Plain JavaScript object (high-level, auto-stringifies to JSON)
    ///
    /// # Returns
    ///
    /// A Promise that resolves to the offset ID when the server acknowledges the record.
    #[napi(ts_return_type = "Promise<bigint>")]
    #[allow(deprecated)]
    pub fn ingest_record(&self, env: Env, payload: Unknown) -> Result<JsObject> {
        let record_payload = convert_js_to_record_payload(&env, payload)?;
        let stream = self.inner.clone();

        env.execute_tokio_future(
            async move {
                let offset = {
                    let mut guard = stream.lock().await;
                    let stream_ref = guard
                        .as_mut()
                        .ok_or_else(|| napi::Error::from_reason("Stream has been closed"))?;
                    stream_ref
                        .ingest_record_offset(record_payload)
                        .await
                        .map_err(|e| {
                            napi::Error::from_reason(format!("Failed to ingest record: {}", e))
                        })?
                };
                {
                    let guard = stream.lock().await;
                    let stream_ref = guard
                        .as_ref()
                        .ok_or_else(|| napi::Error::from_reason("Stream has been closed"))?;
                    stream_ref.wait_for_offset(offset).await.map_err(|e| {
                        napi::Error::from_reason(format!("Acknowledgment failed: {}", e))
                    })?;
                }
                Ok(offset)
            },
            |env, offset_id| {
                let global: JsGlobal = env.get_global()?;
                let bigint_ctor: JsFunction = global.get_named_property("BigInt")?;
                let js_str = env.create_string(&offset_id.to_string())?;
                bigint_ctor.call(None, &[js_str.into_unknown()])
            },
        )
    }

    /// Ingests multiple records as a single atomic batch.
    ///
    /// **@deprecated** Use `ingestRecordsOffset()` instead, which returns the offset directly
    /// after queuing. Then use `waitForOffset()` to wait for acknowledgment when needed.
    ///
    /// This method accepts an array of records (Protocol Buffer buffers or JSON strings)
    /// and ingests them as a batch. The batch receives a single acknowledgment from
    /// the server with all-or-nothing semantics.
    ///
    /// Similar to ingestRecord(), this BLOCKS until the batch is sent to the SDK's
    /// internal landing zone, then returns a Promise for the server acknowledgment.
    ///
    /// # Arguments
    ///
    /// * `records` - Array of record data (Buffer for protobuf, string for JSON)
    ///
    /// # Returns
    ///
    /// Promise resolving to:
    /// - `bigint`: offset ID for non-empty batches
    /// - `null`: for empty batches
    ///
    /// # Example
    ///
    /// ```typescript
    /// const buffers = records.map(r => Buffer.from(encode(r)));
    /// const offsetId = await stream.ingestRecords(buffers);
    ///
    /// if (offsetId !== null) {
    ///   console.log('Batch acknowledged at offset:', offsetId);
    /// }
    /// ```
    #[napi(ts_return_type = "Promise<bigint | null>")]
    #[allow(deprecated)]
    pub fn ingest_records(&self, env: Env, records: Vec<Unknown>) -> Result<JsObject> {
        // Rust SDK 2.0 removed the blocking `ingest_records`. v1 semantics
        // (Promise resolves after server ack; `null` for empty batches) are
        // preserved via `ingest_records_offset` + `wait_for_offset`.
        let record_payloads: Result<Vec<RustRecordPayload>> = records
            .into_iter()
            .map(|payload| convert_js_to_record_payload(&env, payload))
            .collect();
        let record_payloads = record_payloads?;
        let stream = self.inner.clone();

        env.execute_tokio_future(
            async move {
                let offset_opt = {
                    let mut guard = stream.lock().await;
                    let stream_ref = guard
                        .as_mut()
                        .ok_or_else(|| napi::Error::from_reason("Stream has been closed"))?;
                    stream_ref
                        .ingest_records_offset(record_payloads)
                        .await
                        .map_err(|e| {
                            napi::Error::from_reason(format!("Failed to ingest batch: {}", e))
                        })?
                };
                if let Some(offset) = offset_opt {
                    let guard = stream.lock().await;
                    let stream_ref = guard
                        .as_ref()
                        .ok_or_else(|| napi::Error::from_reason("Stream has been closed"))?;
                    stream_ref.wait_for_offset(offset).await.map_err(|e| {
                        napi::Error::from_reason(format!("Batch acknowledgment failed: {}", e))
                    })?;
                }
                Ok(offset_opt)
            },
            |env, result| match result {
                Some(offset_id) => {
                    let global: JsGlobal = env.get_global()?;
                    let bigint_ctor: JsFunction = global.get_named_property("BigInt")?;
                    let js_str = env.create_string(&offset_id.to_string())?;
                    let bigint = bigint_ctor.call(None, &[js_str.into_unknown()])?;
                    Ok(bigint.into_unknown())
                }
                None => env.get_null().map(|v| v.into_unknown()),
            },
        )
    }

    /// Ingests a single record and returns a future that resolves to the offset ID after queuing.
    ///
    /// Unlike `ingestRecord()`, this method's Promise resolves immediately after
    /// the record is queued, without waiting for server acknowledgment. Use
    /// `waitForOffset()` to wait for acknowledgment when needed.
    ///
    /// This is the recommended API for high-throughput scenarios where you want to
    /// decouple record ingestion from acknowledgment tracking.
    ///
    /// **Acknowledgments:** the idiomatic flow is to ingest in a loop and then `flush()`
    /// once to confirm everything queued so far. The returned offset, together with
    /// `waitForOffset()`, lets you confirm a specific record when you need it (acks are
    /// ordered, so the last offset confirms the whole run) — prefer `flush()` for bulk.
    /// Avoid calling `waitForOffset()` after every record in a tight loop, since that
    /// limits throughput to one record per round-trip.
    ///
    /// # Arguments
    ///
    /// * `payload` - The record data (Buffer, string, protobuf message, or plain object)
    ///
    /// # Returns
    ///
    /// `Promise<bigint>` - Resolves to the offset ID immediately after the record is queued
    /// (does not wait for server acknowledgment).
    ///
    /// # Example
    ///
    /// ```typescript
    /// // High-throughput pattern: ingest in a loop, wait once at the end.
    /// let lastOffset;
    /// for (const record of records) {
    ///   lastOffset = await stream.ingestRecordOffset(record); // resolves on queue, no round-trip
    /// }
    /// // The ack watermark is monotonic: waiting on the last offset confirms all prior records.
    /// await stream.waitForOffset(lastOffset);
    /// // Or simply: await stream.flush();
    /// ```
    #[napi(ts_return_type = "Promise<bigint>")]
    pub fn ingest_record_offset(&self, env: Env, payload: Unknown) -> Result<JsObject> {
        let record_payload = convert_js_to_record_payload(&env, payload)?;

        let stream = self.inner.clone();

        env.execute_tokio_future(
            async move {
                let mut guard = stream.lock().await;
                let stream_ref = guard
                    .as_mut()
                    .ok_or_else(|| napi::Error::from_reason("Stream has been closed"))?;

                stream_ref
                    .ingest_record_offset(record_payload)
                    .await
                    .map_err(|e| {
                        napi::Error::from_reason(format!("Failed to ingest record: {}", e))
                    })
            },
            |env, offset_id| {
                let offset_str = offset_id.to_string();
                let global: JsGlobal = env.get_global()?;
                let bigint_ctor: JsFunction = global.get_named_property("BigInt")?;
                let js_str = env.create_string(&offset_str)?;
                bigint_ctor.call(None, &[js_str.into_unknown()])
            },
        )
    }

    /// Ingests multiple records as a batch and returns a future that resolves to the offset ID after queuing.
    ///
    /// Unlike `ingestRecords()`, this method's Promise resolves immediately after
    /// the batch is queued, without waiting for server acknowledgment. Use
    /// `waitForOffset()` to wait for acknowledgment when needed.
    ///
    /// **Acknowledgments:** the idiomatic flow is to ingest your batches in a loop and
    /// then `flush()` once to confirm. The returned offset, together with `waitForOffset()`,
    /// confirms a specific batch when you need it (acks are ordered, so the last offset
    /// confirms the whole run) — prefer `flush()` for bulk. Avoid calling `waitForOffset()`
    /// after every batch in a tight loop, since that limits throughput to one round-trip per batch.
    ///
    /// # Arguments
    ///
    /// * `records` - Array of record data
    ///
    /// # Returns
    ///
    /// `Promise<bigint | null>` - Resolves to the offset ID immediately after the batch
    /// is queued (does not wait for server acknowledgment). Returns null for empty batches.
    ///
    /// # Example
    ///
    /// ```typescript
    /// // Ingest many batches without waiting, then flush once.
    /// let lastOffset = null;
    /// for (const batch of batches) {
    ///   const offset = await stream.ingestRecordsOffset(batch); // resolves on queue
    ///   if (offset !== null) lastOffset = offset;
    /// }
    /// if (lastOffset !== null) await stream.waitForOffset(lastOffset);
    /// // Or simply: await stream.flush();
    /// ```
    #[napi(ts_return_type = "Promise<bigint | null>")]
    pub fn ingest_records_offset(&self, env: Env, records: Vec<Unknown>) -> Result<JsObject> {
        let record_payloads: Result<Vec<RustRecordPayload>> = records
            .into_iter()
            .map(|payload| convert_js_to_record_payload(&env, payload))
            .collect();

        let record_payloads = record_payloads?;

        let stream = self.inner.clone();

        env.execute_tokio_future(
            async move {
                let mut guard = stream.lock().await;
                let stream_ref = guard
                    .as_mut()
                    .ok_or_else(|| napi::Error::from_reason("Stream has been closed"))?;

                stream_ref
                    .ingest_records_offset(record_payloads)
                    .await
                    .map_err(|e| napi::Error::from_reason(format!("Failed to ingest batch: {}", e)))
            },
            |env, result| match result {
                Some(offset_id) => {
                    let offset_str = offset_id.to_string();
                    let global: JsGlobal = env.get_global()?;
                    let bigint_ctor: JsFunction = global.get_named_property("BigInt")?;
                    let js_str = env.create_string(&offset_str)?;
                    let bigint = bigint_ctor.call(None, &[js_str.into_unknown()])?;
                    Ok(bigint.into_unknown())
                }
                None => env.get_null().map(|v| v.into_unknown()),
            },
        )
    }

    /// Waits for a specific offset to be acknowledged by the server.
    ///
    /// Use this method with `ingestRecordOffset()` and `ingestRecordsOffset()` to confirm
    /// a specific record before continuing. Acks are ordered, so waiting on the LAST offset
    /// confirms every prior record too — you never need to wait on intermediate offsets.
    /// For confirming a bulk run, `flush()` is usually simpler; reach for `waitForOffset()`
    /// when one particular record must be confirmed. Avoid calling it after every record in
    /// a tight loop, since that limits throughput to one record per round-trip.
    ///
    /// # Arguments
    ///
    /// * `offset_id` - The offset ID to wait for (returned by ingestRecordOffset/ingestRecordsOffset)
    ///
    /// # Errors
    ///
    /// - Timeout if acknowledgment takes too long
    /// - Server errors propagated immediately (no waiting for timeout)
    ///
    /// # Example
    ///
    /// ```typescript
    /// let lastOffset;
    /// for (const record of records) {
    ///   lastOffset = await stream.ingestRecordOffset(record); // no per-record wait
    /// }
    /// // Wait for the last offset only (implies all previous are also acknowledged).
    /// await stream.waitForOffset(lastOffset);
    /// ```
    #[napi(ts_return_type = "Promise<void>")]
    pub fn wait_for_offset(&self, env: Env, offset_id: BigInt) -> Result<JsObject> {
        let offset = bigint_to_i64(offset_id)?;

        let stream = self.inner.clone();

        env.execute_tokio_future(
            async move {
                let guard = stream.lock().await;
                let stream_ref = guard
                    .as_ref()
                    .ok_or_else(|| napi::Error::from_reason("Stream has been closed"))?;

                stream_ref.wait_for_offset(offset).await.map_err(|e| {
                    napi::Error::from_reason(format!("Failed to wait for offset: {}", e))
                })
            },
            |_env, _| Ok(()),
        )
    }

    /// Flushes all pending records and waits for acknowledgments.
    ///
    /// This method ensures all previously ingested records have been sent to the server
    /// and acknowledged. It's useful for checkpointing or ensuring data durability.
    ///
    /// This is the idiomatic way to confirm records ingested via `ingestRecordOffset()` /
    /// `ingestRecordsOffset()`: ingest in a loop, then `flush()` once (for a bounded batch,
    /// or periodically for a long-running stream). It resolves once everything queued so
    /// far is acknowledged.
    ///
    /// # Errors
    ///
    /// - Timeout errors if flush takes longer than configured timeout
    /// - Network errors if the connection fails during flush
    #[napi]
    pub async fn flush(&self) -> Result<()> {
        let guard = self.inner.lock().await;
        let stream = guard
            .as_ref()
            .ok_or_else(|| Error::from_reason("Stream has been closed"))?;

        stream
            .flush()
            .await
            .map_err(|e| Error::from_reason(format!("Failed to flush stream: {}", e)))
    }

    /// Closes the stream gracefully.
    ///
    /// This method flushes all pending records, waits for acknowledgments, and then
    /// closes the underlying gRPC connection. Always call this method when done with
    /// the stream to ensure data integrity.
    ///
    /// # Errors
    ///
    /// - Returns an error if some records could not be acknowledged
    /// - Network errors during the close operation
    #[napi]
    pub async fn close(&self) -> Result<()> {
        let mut guard = self.inner.lock().await;
        if let Some(mut stream) = guard.take() {
            stream
                .close()
                .await
                .map_err(|e| Error::from_reason(format!("Failed to close stream: {}", e)))?;
        }
        Ok(())
    }

    /// Gets the list of unacknowledged records.
    ///
    /// This method should only be called after a stream failure to retrieve records
    /// that were sent but not acknowledged by the server. These records can be
    /// re-ingested into a new stream.
    ///
    /// # Returns
    ///
    /// An array of Buffers containing the unacknowledged record payloads.
    #[napi]
    pub async fn get_unacked_records(&self) -> Result<Vec<Buffer>> {
        let guard = self.inner.lock().await;
        let stream = guard
            .as_ref()
            .ok_or_else(|| Error::from_reason("Stream has been closed"))?;

        let unacked = stream
            .get_unacked_records()
            .await
            .map_err(|e| Error::from_reason(format!("Failed to get unacked records: {}", e)))?;

        Ok(unacked
            .into_iter()
            .map(|payload| match payload {
                RustRecordPayload::Proto(vec) => vec.into(),
                RustRecordPayload::Json(s) => s.into_bytes().into(),
            })
            .collect())
    }

    /// Gets unacknowledged records grouped by their original batches.
    ///
    /// This preserves the batch structure from ingestion:
    /// - Each ingestRecord() call → 1-element batch
    /// - Each ingestRecords() call → N-element batch
    ///
    /// Should only be called after stream failure. All records returned as Buffers
    /// (JSON strings are converted to UTF-8 bytes).
    ///
    /// # Returns
    ///
    /// Array of batches, where each batch is an array of Buffers
    ///
    /// # Example
    ///
    /// ```typescript
    /// try {
    ///   await stream.ingestRecords(batch1);
    ///   await stream.ingestRecords(batch2);
    /// } catch (error) {
    ///   const unackedBatches = await stream.getUnackedBatches();
    ///
    ///   // Re-ingest with new stream
    ///   for (const batch of unackedBatches) {
    ///     await newStream.ingestRecords(batch);
    ///   }
    /// }
    /// ```
    #[napi]
    pub async fn get_unacked_batches(&self) -> Result<Vec<Vec<Buffer>>> {
        let guard = self.inner.lock().await;
        let stream = guard
            .as_ref()
            .ok_or_else(|| Error::from_reason("Stream has been closed"))?;

        let unacked_batches = stream
            .get_unacked_batches()
            .await
            .map_err(|e| Error::from_reason(format!("Failed to get unacked batches: {}", e)))?;

        Ok(unacked_batches
            .into_iter()
            .map(|batch| {
                batch
                    .into_iter()
                    .map(|record| match record {
                        RustRecordPayload::Proto(vec) => vec.into(),
                        RustRecordPayload::Json(s) => s.into_bytes().into(),
                    })
                    .collect()
            })
            .collect())
    }
}

/// JavaScript headers provider callback wrapper.
///
/// Allows TypeScript code to provide custom authentication headers
/// by implementing a getHeaders() function.
#[napi(object)]
pub struct JsHeadersProvider {
    /// JavaScript function: () => Promise<Array<[string, string]>>
    pub get_headers_callback: JsFunction,
}

/// Internal adapter that wraps static headers as a HeadersProvider
/// This is used for custom authentication in the TypeScript SDK
struct StaticHeadersProvider {
    headers: HashMap<&'static str, String>,
}

impl StaticHeadersProvider {
    fn new(headers: Vec<(String, String)>) -> RustZerobusResult<Self> {
        // Convert Vec<(String, String)> to HashMap<&'static str, String>
        // We need to leak strings to get 'static lifetime for keys
        let mut map = HashMap::new();
        for (k, v) in headers {
            let static_key: &'static str = Box::leak(k.into_boxed_str());
            map.insert(static_key, v);
        }

        if !map.contains_key("authorization") {
            return Err(RustZerobusError::InvalidArgument(
                "HeadersProvider must include 'authorization' header with Bearer token".to_string(),
            ));
        }
        if !map.contains_key("x-databricks-zerobus-table-name") {
            return Err(RustZerobusError::InvalidArgument(
                "HeadersProvider must include 'x-databricks-zerobus-table-name' header".to_string(),
            ));
        }

        // Add TS user agent if not provided
        if !map.contains_key("user-agent") {
            map.insert("user-agent", TS_SDK_USER_AGENT.to_string());
        }

        Ok(Self { headers: map })
    }
}

#[async_trait]
impl RustHeadersProvider for StaticHeadersProvider {
    async fn get_headers(&self) -> RustZerobusResult<HashMap<&'static str, String>> {
        Ok(self.headers.clone())
    }
}

/// Helper to create a threadsafe function from JavaScript callback
fn create_headers_tsfn(
    js_func: JsFunction,
) -> Result<ThreadsafeFunction<(), ErrorStrategy::Fatal>> {
    js_func.create_threadsafe_function(0, |ctx| Ok(vec![ctx.value]))
}

/// Helper to call headers callback and get result
async fn call_headers_tsfn(
    tsfn: ThreadsafeFunction<(), ErrorStrategy::Fatal>,
) -> Result<Vec<(String, String)>> {
    let raw_headers: Vec<Vec<String>> = tsfn
        .call_async(())
        .await
        .map_err(|e| Error::from_reason(format!("Failed to call headers callback: {}", e)))?;

    let headers: Vec<(String, String)> = raw_headers
        .into_iter()
        .filter_map(|pair| {
            if pair.len() >= 2 {
                Some((pair[0].clone(), pair[1].clone()))
            } else {
                None
            }
        })
        .collect();

    Ok(headers)
}

/// OAuth headers provider for TypeScript SDK.
/// Uses the Rust SDK's token factory but with the TS user agent.
struct TsOAuthHeadersProvider {
    client_id: String,
    client_secret: String,
    table_name: String,
    workspace_id: String,
    unity_catalog_url: String,
}

impl TsOAuthHeadersProvider {
    fn new(
        client_id: String,
        client_secret: String,
        table_name: String,
        workspace_id: String,
        unity_catalog_url: String,
    ) -> Self {
        Self {
            client_id,
            client_secret,
            table_name,
            workspace_id,
            unity_catalog_url,
        }
    }
}

#[async_trait]
impl RustHeadersProvider for TsOAuthHeadersProvider {
    async fn get_headers(&self) -> RustZerobusResult<HashMap<&'static str, String>> {
        let token = DefaultTokenFactory::get_token(
            &self.unity_catalog_url,
            &self.table_name,
            &self.client_id,
            &self.client_secret,
            &self.workspace_id,
        )
        .await?;

        let mut headers = HashMap::new();
        headers.insert("authorization", format!("Bearer {}", token));
        headers.insert("x-databricks-zerobus-table-name", self.table_name.clone());
        headers.insert("user-agent", TS_SDK_USER_AGENT.to_string());
        Ok(headers)
    }
}

/// The main SDK for interacting with the Databricks Zerobus service.
///
/// This is the entry point for creating ingestion streams to Delta tables.
///
/// # Example
///
/// ```typescript
/// const sdk = new ZerobusSdk(
///   "https://workspace-id.zerobus.region.cloud.databricks.com",
///   "https://workspace.cloud.databricks.com"
/// );
///
/// const stream = await sdk.createStream(
///   { tableName: "catalog.schema.table" },
///   "client-id",
///   "client-secret"
/// );
/// ```
#[napi]
pub struct ZerobusSdk {
    inner: Arc<RustZerobusSdk>,
    /// Stored for creating TsOAuthHeadersProvider
    workspace_id: String,
    /// Stored for creating TsOAuthHeadersProvider
    unity_catalog_url: String,
}

#[napi]
impl ZerobusSdk {
    /// Creates a new Zerobus SDK instance.
    ///
    /// # Arguments
    ///
    /// * `zerobus_endpoint` - The Zerobus API endpoint URL
    ///   (e.g., "https://workspace-id.zerobus.region.cloud.databricks.com")
    /// * `unity_catalog_url` - The Unity Catalog endpoint URL
    ///   (e.g., "https://workspace.cloud.databricks.com")
    ///
    /// # Errors
    ///
    /// - Invalid endpoint URLs
    /// - Failed to extract workspace ID from the endpoint
    #[napi(constructor)]
    pub fn new(zerobus_endpoint: String, unity_catalog_url: String) -> Result<Self> {
        let workspace_id = zerobus_endpoint
            .strip_prefix("https://")
            .or_else(|| zerobus_endpoint.strip_prefix("http://"))
            .and_then(|s| s.split('.').next())
            .map(|s| s.to_string())
            .ok_or_else(|| {
                Error::from_reason(
                    "Failed to extract workspace_id from zerobus_endpoint".to_string(),
                )
            })?;

        let inner = RustZerobusSdk::builder()
            .endpoint(&zerobus_endpoint)
            .unity_catalog_url(&unity_catalog_url)
            .build()
            .map_err(|e| Error::from_reason(format!("Failed to create SDK: {}", e)))?;

        Ok(ZerobusSdk {
            inner: Arc::new(inner),
            workspace_id,
            unity_catalog_url,
        })
    }

    /// Creates a new ingestion stream to a Delta table.
    ///
    /// This method establishes a bidirectional gRPC connection to the Zerobus service
    /// and prepares it for data ingestion. By default, it uses OAuth 2.0 Client Credentials
    /// authentication. For custom authentication (e.g., Personal Access Tokens), provide
    /// a custom headers_provider.
    ///
    /// # Arguments
    ///
    /// * `table_properties` - Properties of the target table including name and optional schema
    /// * `client_id` - OAuth 2.0 client ID (ignored if headers_provider is provided)
    /// * `client_secret` - OAuth 2.0 client secret (ignored if headers_provider is provided)
    /// * `options` - Optional stream configuration (timeouts, recovery settings, etc.)
    /// * `headers_provider` - Optional custom headers provider for authentication.
    ///   If not provided, uses OAuth with client_id and client_secret.
    ///
    /// # Returns
    ///
    /// A Promise that resolves to a ZerobusStream ready for data ingestion.
    ///
    /// # Errors
    ///
    /// - Authentication failures (invalid credentials)
    /// - Invalid table name or insufficient permissions
    /// - Network connectivity issues
    /// - Schema validation errors
    ///
    /// # Example
    ///
    /// ```typescript
    /// // OAuth authentication (default)
    /// const stream = await sdk.createStream(
    ///   { tableName: "catalog.schema.table" },
    ///   "client-id",
    ///   "client-secret"
    /// );
    ///
    /// // Custom authentication with headers provider
    /// const stream = await sdk.createStream(
    ///   { tableName: "catalog.schema.table" },
    ///   "", // ignored
    ///   "", // ignored
    ///   undefined,
    ///   {
    ///     getHeadersCallback: async () => [
    ///       ["authorization", `Bearer ${myToken}`],
    ///       ["x-databricks-zerobus-table-name", tableName]
    ///     ]
    ///   }
    /// );
    /// ```
    #[napi(ts_return_type = "Promise<ZerobusStream>")]
    pub fn create_stream(
        &self,
        env: Env,
        table_properties: TableProperties,
        client_id: String,
        client_secret: String,
        options: Option<StreamConfigurationOptions>,
        headers_provider: Option<JsHeadersProvider>,
    ) -> Result<JsObject> {
        // Decode the optional protobuf descriptor up-front so we can hand it
        // to the builder's `.compiled_proto()` setter; the builder constructs
        // the (now-private) `TableProperties` itself.
        let descriptor_proto: Option<prost_types::DescriptorProto> =
            if let Some(ref desc_str) = table_properties.descriptor_proto {
                let bytes = base64_decode(desc_str).map_err(|e| {
                    Error::from_reason(format!("Failed to decode descriptor: {}", e))
                })?;
                let dp: prost_types::DescriptorProto = prost::Message::decode(&bytes[..])
                    .map_err(|e| {
                        Error::from_reason(format!("Failed to parse descriptor proto: {}", e))
                    })?;
                Some(dp)
            } else {
                None
            };

        let opts = options.unwrap_or(StreamConfigurationOptions {
            max_inflight_requests: None,
            recovery: None,
            recovery_timeout_ms: None,
            recovery_backoff_ms: None,
            recovery_retries: None,
            flush_timeout_ms: None,
            server_lack_of_ack_timeout_ms: None,
            record_type: None,
            stream_paused_max_wait_time_ms: None,
        });

        let record_type = match opts.record_type {
            Some(0) => RustRecordType::Json,
            Some(1) => RustRecordType::Proto,
            _ => RustRecordType::Proto,
        };

        let headers_tsfn = match headers_provider {
            Some(JsHeadersProvider {
                get_headers_callback,
            }) => Some(create_headers_tsfn(get_headers_callback)?),
            None => None,
        };

        let sdk = self.inner.clone();
        let workspace_id = self.workspace_id.clone();
        let unity_catalog_url = self.unity_catalog_url.clone();
        let table_name = table_properties.table_name.clone();

        env.execute_tokio_future(
            async move {
                let headers_provider_arc: Arc<dyn RustHeadersProvider> = if let Some(tsfn) =
                    headers_tsfn
                {
                    let headers = call_headers_tsfn(tsfn).await.map_err(|e| {
                        napi::Error::from_reason(format!("Headers callback failed: {}", e))
                    })?;
                    let static_provider = StaticHeadersProvider::new(headers)
                        .map_err(|e| napi::Error::from_reason(format!("Invalid headers: {}", e)))?;
                    Arc::new(static_provider)
                } else {
                    Arc::new(TsOAuthHeadersProvider::new(
                        client_id,
                        client_secret,
                        table_name.clone(),
                        workspace_id,
                        unity_catalog_url,
                    ))
                };

                let mut builder = sdk
                    .stream_builder()
                    .table(table_name)
                    .headers_provider(headers_provider_arc);

                if let Some(v) = opts.max_inflight_requests {
                    builder = builder.max_inflight_requests(v as usize);
                }
                if let Some(v) = opts.recovery {
                    builder = builder.recovery(v);
                }
                if let Some(v) = opts.recovery_timeout_ms {
                    builder = builder.recovery_timeout_ms(v as u64);
                }
                if let Some(v) = opts.recovery_backoff_ms {
                    builder = builder.recovery_backoff_ms(v as u64);
                }
                if let Some(v) = opts.recovery_retries {
                    builder = builder.recovery_retries(v);
                }
                if let Some(v) = opts.flush_timeout_ms {
                    builder = builder.flush_timeout_ms(v as u64);
                }
                if let Some(v) = opts.server_lack_of_ack_timeout_ms {
                    builder = builder.server_lack_of_ack_timeout_ms(v as u64);
                }
                if let Some(v) = opts.stream_paused_max_wait_time_ms {
                    builder = builder.stream_paused_max_wait_time_ms(Some(v as u64));
                }

                let builder = match record_type {
                    RustRecordType::Json => builder.json(),
                    RustRecordType::Proto | RustRecordType::Unspecified => {
                        let desc = descriptor_proto.ok_or_else(|| {
                            napi::Error::from_reason(
                                "Proto record type requires descriptor_proto on TableProperties",
                            )
                        })?;
                        builder.compiled_proto(desc)
                    }
                };

                let stream = builder.build().await.map_err(|e| {
                    napi::Error::from_reason(format!("Failed to create stream: {}", e))
                })?;

                Ok(ZerobusStream {
                    inner: Arc::new(Mutex::new(Some(stream))),
                })
            },
            |_env, stream| Ok(stream),
        )
    }

    /// Recreates a stream with the same configuration and re-ingests unacknowledged batches.
    ///
    /// This method is the recommended approach for recovering from stream failures. It:
    /// 1. Retrieves all unacknowledged batches from the failed stream
    /// 2. Creates a new stream with identical configuration
    /// 3. Re-ingests all unacknowledged batches in order
    /// 4. Returns the new stream ready for continued ingestion
    ///
    /// # Arguments
    ///
    /// * `stream` - The failed or closed stream to recreate
    ///
    /// # Returns
    ///
    /// A Promise that resolves to a new ZerobusStream with all unacknowledged batches re-ingested.
    ///
    /// # Errors
    ///
    /// - Failed to retrieve unacknowledged batches from the original stream
    /// - Authentication failures when creating the new stream
    /// - Network connectivity issues during re-ingestion
    ///
    /// # Examples
    ///
    /// ```typescript
    /// try {
    ///   await stream.ingestRecords(batch);
    /// } catch (error) {
    ///   await stream.close();
    ///   // Recreate stream with all unacked batches re-ingested
    ///   const newStream = await sdk.recreateStream(stream);
    ///   // Continue ingesting with newStream
    /// }
    /// ```
    #[napi]
    pub async fn recreate_stream(&self, stream: &ZerobusStream) -> Result<ZerobusStream> {
        let inner_guard = stream.inner.lock().await;
        let rust_stream = inner_guard
            .as_ref()
            .ok_or_else(|| Error::from_reason("Stream has been closed"))?;

        let new_rust_stream = self
            .inner
            .recreate_stream(rust_stream)
            .await
            .map_err(|e| Error::from_reason(format!("Failed to recreate stream: {}", e)))?;

        Ok(ZerobusStream {
            inner: Arc::new(Mutex::new(Some(new_rust_stream))),
        })
    }
}

/// Helper function to decode base64 strings.
fn base64_decode(input: &str) -> std::result::Result<Vec<u8>, String> {
    use base64::{engine::general_purpose::STANDARD, Engine};
    STANDARD
        .decode(input)
        .map_err(|e| format!("Base64 decode error: {}", e))
}

// =============================================================================
// Arrow Flight Support (Beta)
// Enabled with feature flag: cargo build --features arrow-flight
// =============================================================================

#[cfg(feature = "arrow-flight")]
use arrow_ipc::writer::StreamWriter;
#[cfg(feature = "arrow-flight")]
use bytes::Bytes;
#[cfg(feature = "arrow-flight")]
use databricks_zerobus_ingest_sdk::{
    ArrowSchema as RustArrowSchema, DataType as RustDataType, Field as RustField,
    RecordBatch as RustRecordBatch, ZerobusArrowStream as RustZerobusArrowStream,
};

/// IPC compression type for Arrow Flight streams.
///
/// **Beta**: Arrow Flight support is in Beta. The API is stabilising but
/// may still change before reaching GA.
#[cfg(feature = "arrow-flight")]
#[napi]
pub enum IpcCompressionType {
    /// LZ4 frame compression - fast compression with moderate ratio
    Lz4Frame = 0,
    /// Zstandard compression - better compression ratio, slightly slower
    Zstd = 1,
}

/// Configuration options for Arrow Flight streams.
///
/// **Beta**: Arrow Flight support is in Beta. The API is stabilising but
/// may still change before reaching GA.
#[cfg(feature = "arrow-flight")]
#[napi(object)]
#[derive(Debug, Clone)]
pub struct ArrowStreamConfigurationOptions {
    /// Maximum number of batches that can be in-flight (sent but not acknowledged).
    /// Default: 1,000
    pub max_inflight_batches: Option<u32>,

    /// Whether to enable automatic stream recovery on failure.
    /// Default: true
    pub recovery: Option<bool>,

    /// Timeout for recovery operations in milliseconds.
    /// Default: 15,000 (15 seconds)
    pub recovery_timeout_ms: Option<u32>,

    /// Delay between recovery retry attempts in milliseconds.
    /// Default: 2,000 (2 seconds)
    pub recovery_backoff_ms: Option<u32>,

    /// Maximum number of recovery attempts before giving up.
    /// Default: 4
    pub recovery_retries: Option<u32>,

    /// Timeout waiting for server acknowledgments in milliseconds.
    /// Default: 60,000 (1 minute)
    pub server_lack_of_ack_timeout_ms: Option<u32>,

    /// Timeout for flush operations in milliseconds.
    /// Default: 300,000 (5 minutes)
    pub flush_timeout_ms: Option<u32>,

    /// Timeout for connection establishment in milliseconds.
    /// Default: 30,000 (30 seconds)
    pub connection_timeout_ms: Option<u32>,

    /// Optional IPC compression type (0 = LZ4Frame, 1 = Zstd, undefined = no compression)
    pub ipc_compression: Option<i32>,
}

// Rust SDK 2.0 `ArrowStreamConfigurationOptions` is `#[non_exhaustive]` and
// cannot be constructed via struct literal from this crate. Arrow options are
// applied via setters on `sdk.stream_builder()` inside `create_arrow_stream`
// below.

#[cfg(feature = "arrow-flight")]
fn map_ipc_compression(value: Option<i32>) -> Option<arrow_ipc::CompressionType> {
    match value {
        Some(0) => Some(arrow_ipc::CompressionType::LZ4_FRAME),
        Some(1) => Some(arrow_ipc::CompressionType::ZSTD),
        _ => None,
    }
}

/// Arrow data type enum for schema definition.
///
/// **Beta**: Arrow Flight support is in Beta.
#[cfg(feature = "arrow-flight")]
#[napi]
pub enum ArrowDataType {
    /// Boolean type
    Boolean = 0,
    /// Signed 8-bit integer
    Int8 = 1,
    /// Signed 16-bit integer
    Int16 = 2,
    /// Signed 32-bit integer
    Int32 = 3,
    /// Signed 64-bit integer
    Int64 = 4,
    /// Unsigned 8-bit integer
    UInt8 = 5,
    /// Unsigned 16-bit integer
    UInt16 = 6,
    /// Unsigned 32-bit integer
    UInt32 = 7,
    /// Unsigned 64-bit integer
    UInt64 = 8,
    /// 32-bit floating point
    Float32 = 9,
    /// 64-bit floating point
    Float64 = 10,
    /// UTF-8 encoded string
    Utf8 = 11,
    /// Large UTF-8 encoded string (64-bit offsets)
    LargeUtf8 = 12,
    /// Binary data
    Binary = 13,
    /// Large binary data (64-bit offsets)
    LargeBinary = 14,
    /// Date (32-bit days since epoch)
    Date32 = 15,
    /// Date (64-bit milliseconds since epoch)
    Date64 = 16,
    /// Timestamp with microsecond precision (UTC)
    TimestampMicros = 17,
    /// Timestamp with nanosecond precision (UTC)
    TimestampNanos = 18,
}

#[cfg(feature = "arrow-flight")]
fn convert_arrow_data_type(dt: i32) -> RustDataType {
    match dt {
        0 => RustDataType::Boolean,
        1 => RustDataType::Int8,
        2 => RustDataType::Int16,
        3 => RustDataType::Int32,
        4 => RustDataType::Int64,
        5 => RustDataType::UInt8,
        6 => RustDataType::UInt16,
        7 => RustDataType::UInt32,
        8 => RustDataType::UInt64,
        9 => RustDataType::Float32,
        10 => RustDataType::Float64,
        11 => RustDataType::Utf8,
        12 => RustDataType::LargeUtf8,
        13 => RustDataType::Binary,
        14 => RustDataType::LargeBinary,
        15 => RustDataType::Date32,
        16 => RustDataType::Date64,
        17 => RustDataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into())),
        18 => RustDataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, Some("UTC".into())),
        _ => RustDataType::Utf8,
    }
}

/// Arrow field definition for schema.
///
/// **Beta**: Arrow Flight support is in Beta.
#[cfg(feature = "arrow-flight")]
#[napi(object)]
#[derive(Debug, Clone)]
pub struct ArrowField {
    /// Field name
    pub name: String,
    /// Field data type (ArrowDataType enum value)
    pub data_type: i32,
    /// Whether the field is nullable
    pub nullable: Option<bool>,
}

/// Properties of the target Delta table for Arrow Flight ingestion.
///
/// Unlike `TableProperties` which uses Protocol Buffers, Arrow Flight streams
/// require an Arrow schema definition.
///
/// **Beta**: Arrow Flight support is in Beta. The API is stabilising but
/// may still change before reaching GA.
#[cfg(feature = "arrow-flight")]
#[napi(object)]
#[derive(Debug, Clone)]
pub struct ArrowTableProperties {
    /// Full table name in Unity Catalog (e.g., "catalog.schema.table")
    pub table_name: String,
    /// Arrow schema fields
    pub schema_fields: Vec<ArrowField>,
}

// Rust SDK 2.0 made `ArrowTableProperties` non-public; the table name and
// schema are passed to `sdk.stream_builder().table(...).arrow(schema)`
// directly. This helper builds just the Arrow `Schema`.
#[cfg(feature = "arrow-flight")]
fn build_arrow_schema(fields: &[ArrowField]) -> Arc<RustArrowSchema> {
    let rust_fields: Vec<RustField> = fields
        .iter()
        .map(|f| {
            RustField::new(
                &f.name,
                convert_arrow_data_type(f.data_type),
                f.nullable.unwrap_or(true),
            )
        })
        .collect();
    Arc::new(RustArrowSchema::new(rust_fields))
}

/// An Arrow Flight stream for ingesting Arrow RecordBatches into a Delta table.
///
/// This stream provides a high-performance interface for streaming Arrow data
/// to Databricks Delta tables using the Arrow Flight protocol.
///
/// **Beta**: Arrow Flight support is in Beta. The API is stabilising but
/// may still change before reaching GA.
///
/// # Lifecycle
///
/// 1. Create a stream via `sdk.createArrowStream()`
/// 2. Ingest Arrow IPC buffers with `ingestBatch()`
/// 3. Use `waitForOffset()` to wait for acknowledgments
/// 4. Call `flush()` to ensure all batches are persisted
/// 5. Close the stream with `close()`
///
/// # Example
///
/// ```typescript
/// import { tableToIPC } from 'apache-arrow';
///
/// const arrowStream = await sdk.createArrowStream(
///   arrowTableProps,
///   clientId,
///   clientSecret,
///   options
/// );
///
/// const ipcBuffer = tableToIPC(arrowTable, 'stream');
/// const offset = await arrowStream.ingestBatch(Buffer.from(ipcBuffer));
/// await arrowStream.waitForOffset(offset);
/// await arrowStream.close();
/// ```
#[cfg(feature = "arrow-flight")]
#[napi]
pub struct ZerobusArrowStream {
    inner: Arc<Mutex<Option<RustZerobusArrowStream>>>,
    schema: Arc<RustArrowSchema>,
}

/// Helper to serialize RecordBatch to Arrow IPC buffer
#[cfg(feature = "arrow-flight")]
fn serialize_batch_to_ipc(batch: &RustRecordBatch) -> Result<Vec<u8>> {
    let mut buffer = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buffer, batch.schema().as_ref())
            .map_err(|e| Error::from_reason(format!("Failed to create Arrow IPC writer: {}", e)))?;
        writer
            .write(batch)
            .map_err(|e| Error::from_reason(format!("Failed to write batch to IPC: {}", e)))?;
        writer
            .finish()
            .map_err(|e| Error::from_reason(format!("Failed to finish IPC stream: {}", e)))?;
    }
    Ok(buffer)
}

#[cfg(feature = "arrow-flight")]
#[napi]
impl ZerobusArrowStream {
    /// Ingests a single Arrow IPC buffer into the stream.
    ///
    /// The buffer should be an Arrow IPC stream format containing one or more RecordBatches.
    /// You can create this using `tableToIPC(table, 'stream')` from the apache-arrow package.
    ///
    /// # Arguments
    ///
    /// * `ipc_buffer` - Arrow IPC stream format buffer
    ///
    /// # Returns
    ///
    /// The offset ID (bigint) assigned to this batch.
    ///
    /// # Example
    ///
    /// ```typescript
    /// const table = tableFromArrays({
    ///   device_name: ['sensor-1'],
    ///   temp: [25],
    ///   humidity: [60]
    /// });
    /// const ipcBuffer = tableToIPC(table, 'stream');
    /// const offset = await stream.ingestBatch(Buffer.from(ipcBuffer));
    /// await stream.waitForOffset(offset);
    /// ```
    #[napi(ts_return_type = "Promise<bigint>")]
    pub fn ingest_batch(&self, env: Env, ipc_buffer: Buffer) -> Result<JsObject> {
        // The Rust SDK's `ingest_ipc_batch` materialises the bytes into a
        // `RecordBatch`, rejects multi-batch streams, and validates the schema
        // against the stream's schema. No need to duplicate any of that here.
        let stream = self.inner.clone();
        let buffer_vec = ipc_buffer.to_vec();

        env.execute_tokio_future(
            async move {
                let mut guard = stream.lock().await;
                let stream_ref = guard
                    .as_mut()
                    .ok_or_else(|| napi::Error::from_reason("Arrow stream has been closed"))?;

                stream_ref
                    .ingest_ipc_batch(Bytes::from(buffer_vec))
                    .await
                    .map_err(|e| {
                        napi::Error::from_reason(format!("Failed to ingest batch: {}", e))
                    })
            },
            |env, offset_id| {
                let global: JsGlobal = env.get_global()?;
                let bigint_ctor: JsFunction = global.get_named_property("BigInt")?;
                let js_str = env.create_string(&offset_id.to_string())?;
                bigint_ctor.call(None, &[js_str.into_unknown()])
            },
        )
    }

    /// Waits for a specific offset to be acknowledged by the server.
    ///
    /// Use this method with `ingestBatch()` to selectively wait for acknowledgments.
    ///
    /// # Arguments
    ///
    /// * `offset_id` - The offset ID to wait for (returned by ingestBatch)
    #[napi(ts_return_type = "Promise<void>")]
    pub fn wait_for_offset(&self, env: Env, offset_id: BigInt) -> Result<JsObject> {
        let offset = bigint_to_i64(offset_id)?;

        let stream = self.inner.clone();

        env.execute_tokio_future(
            async move {
                let guard = stream.lock().await;
                let stream_ref = guard
                    .as_ref()
                    .ok_or_else(|| napi::Error::from_reason("Arrow stream has been closed"))?;

                stream_ref.wait_for_offset(offset).await.map_err(|e| {
                    napi::Error::from_reason(format!("Failed to wait for offset: {}", e))
                })
            },
            |_env, _| Ok(()),
        )
    }

    /// Flushes all pending batches and waits for acknowledgments.
    #[napi]
    pub async fn flush(&self) -> Result<()> {
        let guard = self.inner.lock().await;
        let stream = guard
            .as_ref()
            .ok_or_else(|| Error::from_reason("Arrow stream has been closed"))?;

        stream
            .flush()
            .await
            .map_err(|e| Error::from_reason(format!("Failed to flush arrow stream: {}", e)))
    }

    /// Closes the stream gracefully.
    #[napi]
    pub async fn close(&self) -> Result<()> {
        let mut guard = self.inner.lock().await;
        if let Some(mut stream) = guard.take() {
            stream
                .close()
                .await
                .map_err(|e| Error::from_reason(format!("Failed to close arrow stream: {}", e)))?;
        }
        Ok(())
    }

    /// Returns whether the stream has been closed.
    #[napi(getter)]
    pub fn is_closed(&self) -> bool {
        // Check synchronously using try_lock
        match self.inner.try_lock() {
            Ok(guard) => guard.is_none(),
            Err(_) => false, // If we can't acquire lock, assume not closed
        }
    }

    /// Returns the table name for this stream.
    #[napi(getter)]
    pub fn table_name(&self) -> Result<String> {
        let guard = self
            .inner
            .try_lock()
            .map_err(|_| Error::from_reason("Stream is busy"))?;
        let stream = guard
            .as_ref()
            .ok_or_else(|| Error::from_reason("Stream has been closed"))?;
        Ok(stream.table_name().to_string())
    }

    /// Gets unacknowledged batches as Arrow IPC buffers.
    ///
    /// This method should only be called after a stream failure to retrieve batches
    /// that were sent but not acknowledged. These can be re-ingested into a new stream.
    ///
    /// # Returns
    ///
    /// An array of Buffers containing the unacknowledged batches in Arrow IPC format.
    #[napi]
    pub async fn get_unacked_batches(&self) -> Result<Vec<Buffer>> {
        let guard = self.inner.lock().await;
        let stream = guard
            .as_ref()
            .ok_or_else(|| Error::from_reason("Stream has been closed"))?;

        let batches = stream
            .get_unacked_batches()
            .await
            .map_err(|e| Error::from_reason(format!("Failed to get unacked batches: {}", e)))?;

        // Convert each RecordBatch back to IPC format
        batches
            .iter()
            .map(|batch| {
                let ipc_bytes = serialize_batch_to_ipc(batch)?;
                Ok(ipc_bytes.into())
            })
            .collect()
    }
}

// Add Arrow stream methods to ZerobusSdk
#[cfg(feature = "arrow-flight")]
#[napi]
impl ZerobusSdk {
    /// Creates a new Arrow Flight stream to a Delta table.
    ///
    /// **Beta**: Arrow Flight support is in Beta. The API is stabilising
    /// but may still change before reaching GA.
    ///
    /// This method establishes an Arrow Flight connection to the Zerobus service
    /// for high-performance columnar data ingestion.
    ///
    /// # Arguments
    ///
    /// * `table_properties` - Properties of the target table including name and Arrow schema
    /// * `client_id` - OAuth 2.0 client ID
    /// * `client_secret` - OAuth 2.0 client secret
    /// * `options` - Optional stream configuration
    ///
    /// # Returns
    ///
    /// A Promise that resolves to a ZerobusArrowStream ready for data ingestion.
    ///
    /// # Example
    ///
    /// ```typescript
    /// const tableProps = {
    ///   tableName: 'catalog.schema.table',
    ///   schemaFields: [
    ///     { name: 'device_name', dataType: ArrowDataType.Utf8 },
    ///     { name: 'temp', dataType: ArrowDataType.Int32 },
    ///     { name: 'humidity', dataType: ArrowDataType.Int64 }
    ///   ]
    /// };
    ///
    /// const arrowStream = await sdk.createArrowStream(
    ///   tableProps,
    ///   clientId,
    ///   clientSecret,
    ///   { maxInflightBatches: 100 }
    /// );
    /// ```
    #[napi(ts_return_type = "Promise<ZerobusArrowStream>")]
    pub fn create_arrow_stream(
        &self,
        env: Env,
        table_properties: ArrowTableProperties,
        client_id: String,
        client_secret: String,
        options: Option<ArrowStreamConfigurationOptions>,
    ) -> Result<JsObject> {
        // Rust SDK 2.0 removed the convenience `create_arrow_stream` method;
        // open via `sdk.stream_builder().table(...).oauth(...).arrow(schema)
        // .build_arrow()` and apply options via setters. `ArrowTableProperties`
        // is also private now, so we build just the schema here.
        let schema = build_arrow_schema(&table_properties.schema_fields);
        let table_name = table_properties.table_name.clone();
        let opts = options.unwrap_or(ArrowStreamConfigurationOptions {
            max_inflight_batches: None,
            recovery: None,
            recovery_timeout_ms: None,
            recovery_backoff_ms: None,
            recovery_retries: None,
            server_lack_of_ack_timeout_ms: None,
            flush_timeout_ms: None,
            connection_timeout_ms: None,
            ipc_compression: None,
        });
        let ipc_compression = map_ipc_compression(opts.ipc_compression);
        let schema_for_stream = schema.clone();

        let sdk = self.inner.clone();

        env.execute_tokio_future(
            async move {
                let mut builder = sdk
                    .stream_builder()
                    .table(table_name)
                    .oauth(client_id, client_secret)
                    .arrow(schema.clone());

                if let Some(v) = opts.max_inflight_batches {
                    builder = builder.max_inflight_batches(v as usize);
                }
                if let Some(v) = opts.recovery {
                    builder = builder.recovery(v);
                }
                if let Some(v) = opts.recovery_timeout_ms {
                    builder = builder.recovery_timeout_ms(v as u64);
                }
                if let Some(v) = opts.recovery_backoff_ms {
                    builder = builder.recovery_backoff_ms(v as u64);
                }
                if let Some(v) = opts.recovery_retries {
                    builder = builder.recovery_retries(v);
                }
                if let Some(v) = opts.server_lack_of_ack_timeout_ms {
                    builder = builder.server_lack_of_ack_timeout_ms(v as u64);
                }
                if let Some(v) = opts.flush_timeout_ms {
                    builder = builder.flush_timeout_ms(v as u64);
                }
                if let Some(v) = opts.connection_timeout_ms {
                    builder = builder.connection_timeout_ms(v as u64);
                }
                builder = builder.ipc_compression(ipc_compression);

                let stream = builder.build_arrow().await.map_err(|e| {
                    napi::Error::from_reason(format!("Failed to create arrow stream: {}", e))
                })?;

                Ok(ZerobusArrowStream {
                    inner: Arc::new(Mutex::new(Some(stream))),
                    schema: schema_for_stream,
                })
            },
            |_env, stream| Ok(stream),
        )
    }

    /// Recreates an Arrow stream with the same configuration and re-ingests unacknowledged batches.
    ///
    /// **Beta**: Arrow Flight support is in Beta.
    ///
    /// # Arguments
    ///
    /// * `stream` - The failed or closed Arrow stream to recreate
    ///
    /// # Returns
    ///
    /// A Promise that resolves to a new ZerobusArrowStream with all unacknowledged batches re-ingested.
    #[napi]
    pub async fn recreate_arrow_stream(
        &self,
        stream: &ZerobusArrowStream,
    ) -> Result<ZerobusArrowStream> {
        let inner_guard = stream.inner.lock().await;
        let rust_stream = inner_guard
            .as_ref()
            .ok_or_else(|| Error::from_reason("Arrow stream has been closed"))?;

        let new_rust_stream = self
            .inner
            .recreate_arrow_stream(rust_stream)
            .await
            .map_err(|e| Error::from_reason(format!("Failed to recreate arrow stream: {}", e)))?;

        Ok(ZerobusArrowStream {
            inner: Arc::new(Mutex::new(Some(new_rust_stream))),
            schema: stream.schema.clone(),
        })
    }
}
