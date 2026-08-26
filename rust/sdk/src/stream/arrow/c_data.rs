pub use arrow_array::ffi::FFI_ArrowArray;
pub use arrow_schema::ffi::FFI_ArrowSchema;

use arrow_array::{RecordBatch, RecordBatchOptions, StructArray};
use arrow_schema::{DataType, Schema};
use std::sync::Arc;

use crate::{ZerobusError, ZerobusResult};

/// Imports an owned Arrow C Data Interface record batch.
///
/// # Safety
///
/// `array` and `schema` must satisfy every Arrow C Data Interface invariant.
/// Both values are consumed on success and error.
/// All retained buffers, children, dictionaries, `private_data`, and release
/// callbacks must support asynchronous cross-thread retention until released.
/// Release callbacks may run on any thread that drops the final owner,
/// including SDK runtime/transport threads or a caller performing destructive
/// stream teardown; they must be thread-safe and must not unwind.
pub unsafe fn import_c_data_record_batch(
    array: FFI_ArrowArray,
    schema: FFI_ArrowSchema,
) -> ZerobusResult<RecordBatch> {
    if array.is_released() || schema.release.is_none() {
        return Err(ZerobusError::InvalidArgument(
            "Arrow C Data input has already been released".to_string(),
        ));
    }

    let schema = Schema::try_from(&schema).map_err(|error| {
        ZerobusError::InvalidArgument(format!("Invalid Arrow C Data schema: {error}"))
    })?;
    let data_type = DataType::Struct(schema.fields().clone());
    let data =
        unsafe { arrow_array::ffi::from_ffi_and_data_type(array, data_type) }.map_err(|error| {
            ZerobusError::InvalidArgument(format!("Invalid Arrow C Data array: {error}"))
        })?;
    let row_count = data.len();
    let (_, columns, nulls) = StructArray::from(data).into_parts();
    if nulls.is_some_and(|nulls| nulls.null_count() != 0) {
        return Err(ZerobusError::InvalidArgument(
            "Arrow C Data RecordBatch cannot contain null top-level rows".to_string(),
        ));
    }

    RecordBatch::try_new_with_options(
        Arc::new(schema),
        columns,
        &RecordBatchOptions::new().with_row_count(Some(row_count)),
    )
    .map_err(|error| {
        ZerobusError::InvalidArgument(format!("Invalid Arrow C Data RecordBatch: {error}"))
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use arrow_array::builder::NullBufferBuilder;
    use arrow_array::ffi::FFI_ArrowArray;
    use arrow_array::types::Int32Type;
    use arrow_array::{
        Array, ArrayRef, DictionaryArray, Int32Array, ListArray, StringArray, StructArray,
    };
    use arrow_array::{RecordBatch, RecordBatchOptions};
    use arrow_schema::ffi::FFI_ArrowSchema;
    use arrow_schema::{DataType, Field, Fields, Schema};

    use crate::ZerobusError;

    use super::import_c_data_record_batch;

    struct CountingRelease {
        inner_release: Option<unsafe extern "C" fn(*mut FFI_ArrowArray)>,
        inner_private_data: *mut std::ffi::c_void,
        releases: Arc<AtomicUsize>,
    }

    unsafe extern "C" fn counting_release(array: *mut FFI_ArrowArray) {
        let array = unsafe { &mut *array };
        let state = unsafe { Box::from_raw(array.private_data.cast::<CountingRelease>()) };
        array.release = state.inner_release;
        array.private_data = state.inner_private_data;
        state.releases.fetch_add(1, Ordering::SeqCst);
        if let Some(release) = state.inner_release {
            unsafe { release(array) };
        }
    }

    fn count_releases(array: &mut FFI_ArrowArray, releases: Arc<AtomicUsize>) {
        let state = Box::new(CountingRelease {
            inner_release: array.release,
            inner_private_data: array.private_data,
            releases,
        });
        array.release = Some(counting_release);
        array.private_data = Box::into_raw(state).cast();
    }

    fn export_batch(
        schema: &Schema,
        columns: Vec<ArrayRef>,
        releases: Arc<AtomicUsize>,
    ) -> (FFI_ArrowArray, FFI_ArrowSchema) {
        let struct_array = StructArray::new(schema.fields().clone(), columns, None);
        let mut array = FFI_ArrowArray::new(&struct_array.to_data());
        count_releases(&mut array, releases);
        let schema = FFI_ArrowSchema::try_from(schema).unwrap();
        (array, schema)
    }

    #[test]
    fn import_preserves_values_and_schema_metadata() {
        let metadata = HashMap::from([("source".to_string(), "c-data".to_string())]);
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ])
        .with_metadata(metadata.clone());
        let releases = Arc::new(AtomicUsize::new(0));
        let (array, schema_ffi) = export_batch(
            &schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
            ],
            Arc::clone(&releases),
        );

        let batch = unsafe { import_c_data_record_batch(array, schema_ffi) }.unwrap();

        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.schema().metadata(), &metadata);
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap(),
            &Int32Array::from(vec![1, 2, 3])
        );
        assert_eq!(releases.load(Ordering::SeqCst), 0);
        drop(batch);
        assert_eq!(releases.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn imported_owner_survives_clone_and_slice() {
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);
        let releases = Arc::new(AtomicUsize::new(0));
        let (array, schema_ffi) = export_batch(
            &schema,
            vec![Arc::new(Int32Array::from(vec![10, 20, 30]))],
            Arc::clone(&releases),
        );
        let batch = unsafe { import_c_data_record_batch(array, schema_ffi) }.unwrap();
        let clone = batch.clone();
        let slice = batch.slice(1, 2);

        drop(batch);
        drop(clone);
        assert_eq!(releases.load(Ordering::SeqCst), 0);
        drop(slice);
        assert_eq!(releases.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn import_preserves_dictionary_values() {
        let dictionary = DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![0, 1, 0]),
            Arc::new(StringArray::from(vec!["x", "y"])),
        )
        .unwrap();
        let schema = Schema::new(vec![Field::new(
            "category",
            dictionary.data_type().clone(),
            false,
        )]);
        let releases = Arc::new(AtomicUsize::new(0));
        let (array, schema_ffi) = export_batch(
            &schema,
            vec![Arc::new(dictionary.clone())],
            Arc::clone(&releases),
        );

        let batch = unsafe { import_c_data_record_batch(array, schema_ffi) }.unwrap();
        let actual = batch
            .column(0)
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .unwrap();
        assert_eq!(actual, &dictionary);
        drop(batch);
        assert_eq!(releases.load(Ordering::SeqCst), 1);
    }

    fn release_ffi_array(array: &mut FFI_ArrowArray) {
        if let Some(release) = array.release {
            unsafe { release(array) };
        }
    }

    fn release_ffi_schema(schema: &mut FFI_ArrowSchema) {
        if let Some(release) = schema.release {
            unsafe { release(schema) };
        }
    }

    #[test]
    fn import_zero_column_struct_with_explicit_row_count() {
        let schema = Schema::new(Vec::<Field>::new());
        let releases = Arc::new(AtomicUsize::new(0));
        let batch = RecordBatch::try_new_with_options(
            Arc::new(schema.clone()),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(5)),
        )
        .unwrap();
        let struct_array = StructArray::from(batch);
        let mut array = FFI_ArrowArray::new(&struct_array.to_data());
        count_releases(&mut array, Arc::clone(&releases));
        let schema_ffi = FFI_ArrowSchema::try_from(&schema).unwrap();

        let batch = unsafe { import_c_data_record_batch(array, schema_ffi) }.unwrap();

        assert_eq!(batch.num_rows(), 5);
        assert_eq!(batch.num_columns(), 0);
        drop(batch);
        assert_eq!(releases.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn import_rejects_nullable_top_level_rows() {
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);
        let releases = Arc::new(AtomicUsize::new(0));
        let mut nulls = NullBufferBuilder::new(1);
        nulls.append_null();
        let struct_array = StructArray::new(
            schema.fields().clone(),
            vec![Arc::new(Int32Array::from(vec![1]))],
            nulls.finish(),
        );
        let mut array = FFI_ArrowArray::new(&struct_array.to_data());
        count_releases(&mut array, Arc::clone(&releases));
        let schema_ffi = FFI_ArrowSchema::try_from(&schema).unwrap();

        let error = unsafe { import_c_data_record_batch(array, schema_ffi) }.unwrap_err();

        assert!(matches!(error, ZerobusError::InvalidArgument(_)));
        assert!(error
            .to_string()
            .contains("cannot contain null top-level rows"));
        assert_eq!(releases.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn import_rejects_already_released_input() {
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);
        let releases = Arc::new(AtomicUsize::new(0));
        let (mut array, mut schema_ffi) = export_batch(
            &schema,
            vec![Arc::new(Int32Array::from(vec![1]))],
            Arc::clone(&releases),
        );
        release_ffi_array(&mut array);
        release_ffi_schema(&mut schema_ffi);

        let error = unsafe { import_c_data_record_batch(array, schema_ffi) }.unwrap_err();

        assert!(matches!(error, ZerobusError::InvalidArgument(_)));
        assert!(error.to_string().contains("has already been released"));
        assert_eq!(releases.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn import_preserves_nested_struct_list_until_all_batch_owners_drop() {
        let (schema, source_batch) = nested_struct_list_batch();
        let releases = Arc::new(AtomicUsize::new(0));
        let (array, schema_ffi) = export_batch(
            schema.as_ref(),
            source_batch.columns().to_vec(),
            Arc::clone(&releases),
        );
        drop(source_batch);
        drop(schema);

        let batch = unsafe { import_c_data_record_batch(array, schema_ffi) }.unwrap();
        let nested = batch
            .column(1)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(
            nested
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap(),
            &Int32Array::from(vec![1, 2, 3]),
        );
        let lists = nested
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::ListArray>()
            .unwrap();
        assert_eq!(lists.value_offsets(), &[0, 2, 2, 3]);
        assert_eq!(
            lists
                .values()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap(),
            &Int32Array::from(vec![10, 11, 30]),
        );

        let clone = batch.clone();
        let slice = batch.slice(1, 2);
        drop(batch);
        drop(clone);
        assert_eq!(releases.load(Ordering::SeqCst), 0);
        drop(slice);
        assert_eq!(releases.load(Ordering::SeqCst), 1);
    }

    const BENCHMARK_ROW_COUNT: usize = 65_536;
    const BENCHMARK_ITERATIONS: u32 = 1_000;

    fn primitive_batch(row_count: usize) -> (Arc<Schema>, RecordBatch) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from_iter_values(0..row_count as i32)),
                Arc::new(arrow_array::Float64Array::from_iter_values(
                    (0..row_count).map(|value| value as f64),
                )),
            ],
        )
        .unwrap();
        (schema, batch)
    }

    fn string_batch(row_count: usize) -> (Arc<Schema>, RecordBatch) {
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(StringArray::from_iter_values(
                (0..row_count).map(|value| format!("value-{value}")),
            ))],
        )
        .unwrap();
        (schema, batch)
    }

    fn nested_struct_list_batch() -> (Arc<Schema>, RecordBatch) {
        let values = ListArray::from_iter_primitive::<Int32Type, _, _>([
            Some(vec![Some(10), Some(11)]),
            Some(vec![]),
            Some(vec![Some(30)]),
        ]);
        let child_fields: Fields = vec![
            Field::new("inner_id", DataType::Int32, false),
            Field::new("values", values.data_type().clone(), false),
        ]
        .into();
        let nested_type = DataType::Struct(child_fields.clone());
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("nested", nested_type, false),
        ]));
        let nested_array = StructArray::new(
            child_fields,
            vec![Arc::new(Int32Array::from(vec![1, 2, 3])), Arc::new(values)],
            None,
        );
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![100, 200, 300])),
                Arc::new(nested_array),
            ],
        )
        .unwrap();
        (schema, batch)
    }

    fn nested_batch(row_count: usize) -> (Arc<Schema>, RecordBatch) {
        let child_fields: Fields = vec![Field::new("inner_id", DataType::Int32, false)].into();
        let nested_type = DataType::Struct(child_fields.clone());
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("nested", nested_type, false),
        ]));
        let nested_array = StructArray::new(
            child_fields,
            vec![Arc::new(Int32Array::from_iter_values(0..row_count as i32))],
            None,
        );
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from_iter_values(0..row_count as i32)),
                Arc::new(nested_array),
            ],
        )
        .unwrap();
        (schema, batch)
    }

    fn dictionary_batch(row_count: usize) -> (Arc<Schema>, RecordBatch) {
        let dictionary = DictionaryArray::<Int32Type>::try_new(
            Int32Array::from_iter_values((0..row_count).map(|value| (value % 256) as i32)),
            Arc::new(StringArray::from_iter_values(
                (0..256).map(|value| format!("category-{value}")),
            )),
        )
        .unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "category",
            dictionary.data_type().clone(),
            false,
        )]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(dictionary)]).unwrap();
        (schema, batch)
    }

    fn benchmark_conversion_shape(shape: &str, schema: Arc<Schema>, batch: RecordBatch) {
        use std::io::Cursor;
        use std::time::Instant;

        use arrow_ipc::reader::StreamReader;
        use arrow_ipc::writer::StreamWriter;

        assert_eq!(batch.num_rows(), BENCHMARK_ROW_COUNT);

        let mut ipc = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut ipc, schema.as_ref()).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }

        let ipc_started = Instant::now();
        for _ in 0..BENCHMARK_ITERATIONS {
            let mut reader = StreamReader::try_new(Cursor::new(ipc.as_slice()), None).unwrap();
            assert_eq!(
                reader.next().unwrap().unwrap().num_rows(),
                BENCHMARK_ROW_COUNT
            );
        }
        let ipc_elapsed = ipc_started.elapsed();

        let c_data_started = Instant::now();
        for _ in 0..BENCHMARK_ITERATIONS {
            let struct_array = StructArray::from(batch.clone());
            let array = FFI_ArrowArray::new(&struct_array.to_data());
            let schema_ffi = FFI_ArrowSchema::try_from(schema.as_ref()).unwrap();
            let imported = unsafe { import_c_data_record_batch(array, schema_ffi) }.unwrap();
            assert_eq!(imported.num_rows(), BENCHMARK_ROW_COUNT);
        }
        let c_data_elapsed = c_data_started.elapsed();

        eprintln!(
            "shape={shape} ipc_decode_ns_per_iter={} c_data_import_ns_per_iter={}",
            ipc_elapsed.as_nanos() / u128::from(BENCHMARK_ITERATIONS),
            c_data_elapsed.as_nanos() / u128::from(BENCHMARK_ITERATIONS),
        );
    }

    #[test]
    #[ignore = "manual conversion benchmark"]
    fn benchmark_ipc_materialization_against_c_data_import() {
        let (schema, batch) = primitive_batch(BENCHMARK_ROW_COUNT);
        benchmark_conversion_shape("primitive", schema, batch);

        let (schema, batch) = string_batch(BENCHMARK_ROW_COUNT);
        benchmark_conversion_shape("string", schema, batch);

        let (schema, batch) = nested_batch(BENCHMARK_ROW_COUNT);
        benchmark_conversion_shape("nested", schema, batch);

        let (schema, batch) = dictionary_batch(BENCHMARK_ROW_COUNT);
        benchmark_conversion_shape("dictionary", schema, batch);
    }
}
