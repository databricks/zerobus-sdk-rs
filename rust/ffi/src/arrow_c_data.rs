//! C-specific Arrow C Data ABI marker types and boundary tests.

/// Opaque C ABI view of a canonical Arrow C Data Interface ArrowArray.
#[repr(C)]
pub struct CArrowArray {
    _private: [u8; 0],
}

/// Opaque C ABI view of a canonical Arrow C Data Interface ArrowSchema.
#[repr(C)]
pub struct CArrowSchema {
    _private: [u8; 0],
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use arrow_array::{Array, ArrayRef, Int32Array, StructArray};
    use arrow_schema::{Field, Schema};
    use databricks_zerobus_ingest_sdk::internal::arrow_c_data::{FFI_ArrowArray, FFI_ArrowSchema};

    use crate::{zerobus_arrow_stream_ingest_c_data, CArrowArray, CArrowSchema, CResult};

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
    fn ffi_consumes_array_and_schema_before_stream_validation() {
        use std::ptr;

        let schema = Schema::new(vec![Field::new("id", arrow_schema::DataType::Int32, false)]);
        let releases = Arc::new(AtomicUsize::new(0));
        let (mut array, mut schema_ffi) =
            export_batch(&schema, vec![Arc::new(Int32Array::from(vec![1]))], Arc::clone(&releases));
        let mut result = CResult::success();

        let offset = zerobus_arrow_stream_ingest_c_data(
            ptr::null_mut(),
            (&mut array as *mut FFI_ArrowArray).cast::<CArrowArray>(),
            (&mut schema_ffi as *mut FFI_ArrowSchema).cast::<CArrowSchema>(),
            &mut result,
        );

        assert_eq!(offset, -1);
        assert!(!result.success);
        assert!(array.release.is_none());
        assert!(schema_ffi.release.is_none());
        assert_eq!(releases.load(Ordering::SeqCst), 1);
        crate::zerobus_free_error_message(result.error_message);
    }

    #[test]
    fn ffi_null_input_does_not_transfer_the_other_input() {
        use std::ptr;

        let schema = Schema::new(vec![Field::new("id", arrow_schema::DataType::Int32, false)]);
        let releases = Arc::new(AtomicUsize::new(0));
        let (mut array, schema_ffi) =
            export_batch(&schema, vec![Arc::new(Int32Array::from(vec![1]))], Arc::clone(&releases));
        let mut result = CResult::success();

        let offset = zerobus_arrow_stream_ingest_c_data(
            ptr::null_mut(),
            (&mut array as *mut FFI_ArrowArray).cast::<CArrowArray>(),
            ptr::null_mut(),
            &mut result,
        );

        assert_eq!(offset, -1);
        assert!(!result.success);
        assert!(array.release.is_some());
        assert_eq!(releases.load(Ordering::SeqCst), 0);
        drop(array);
        drop(schema_ffi);
        assert_eq!(releases.load(Ordering::SeqCst), 1);
        crate::zerobus_free_error_message(result.error_message);
    }
}
