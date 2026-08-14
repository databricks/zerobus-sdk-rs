use super::*;
use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use databricks_zerobus_ingest_sdk::{NoTlsConfig, ZerobusSdk};

#[allow(dead_code, clippy::single_component_path_imports)]
#[path = "../../../tests/src/mock_arrow_flight.rs"]
mod mock_arrow_flight;

use mock_arrow_flight::{start_mock_flight_server, MockFlightResponse};

#[tokio::test]
async fn close_keeps_unacked_batches_available_for_java_recovery(
) -> Result<(), Box<dyn std::error::Error>> {
    const TABLE_NAME: &str = "test_catalog.test_schema.test_table";

    let (mock_server, server_url) = start_mock_flight_server().await?;
    mock_server
        .inject_responses(
            TABLE_NAME,
            vec![MockFlightResponse::Error {
                status: tonic::Status::invalid_argument("permanent failure"),
                delay_ms: 0,
            }],
        )
        .await;

    let sdk = ZerobusSdk::builder()
        .endpoint(server_url)
        .unity_catalog_url("https://mock-uc.com")
        .tls_config(Arc::new(NoTlsConfig))
        .build()?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("message", DataType::Utf8, true),
    ]));
    let stream = sdk
        .stream_builder()
        .table(TABLE_NAME)
        .no_auth()
        .arrow(Arc::clone(&schema))
        .recovery(false)
        .build_arrow()
        .await?;
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(StringArray::from(vec![Some("unacked")])),
        ],
    )?;

    let offset = stream.ingest_batch(batch).await?;
    assert!(stream.wait_for_offset(offset).await.is_err());

    let handle = NativeArrowStreamHandle::new(stream, String::new(), String::new());
    assert!(close_native_stream(&handle).await.is_err());

    let guard = handle.stream.lock().await;
    let closed_stream = guard
        .as_ref()
        .expect("native close must retain the Rust stream until Java retrieves unacked data");
    let unacked = closed_stream.get_unacked_batches().await?;
    assert_eq!(unacked.len(), 1);

    Ok(())
}
