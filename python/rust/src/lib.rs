#![allow(non_local_definitions)]

use pyo3::prelude::*;

mod arrow;
mod async_wrapper;
mod auth;
mod common;
mod sync_wrapper;

#[pymodule]
fn _zerobus_core(py: Python, m: &PyModule) -> PyResult<()> {
    // Initialize tracing with environment variable support
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .try_init()
        .ok();

    // Initialize pyo3-asyncio with tokio runtime
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_time();
    builder.enable_io();
    pyo3_asyncio::tokio::init(builder);

    // Add common types
    m.add_class::<common::RecordType>()?;
    m.add_class::<common::StreamConfigurationOptions>()?;
    m.add_class::<common::TableProperties>()?;
    m.add_class::<common::AckCallback>()?;

    // Add exception types
    m.add(
        "ZerobusException",
        py.get_type::<common::ZerobusException>(),
    )?;
    m.add(
        "NonRetriableException",
        py.get_type::<common::NonRetriableException>(),
    )?;

    // Add authentication classes
    m.add_class::<auth::HeadersProvider>()?;

    // Add arrow submodule
    let arrow_module = PyModule::new(py, "arrow")?;
    arrow_module.add_class::<arrow::ArrowStreamConfigurationOptions>()?;
    arrow_module.add_class::<arrow::ZerobusArrowStream>()?;
    arrow_module.add_class::<arrow::AsyncZerobusArrowStream>()?;
    m.add_submodule(arrow_module)?;

    // Add sync submodule
    let sync_module = PyModule::new(py, "sync")?;
    sync_module.add_class::<sync_wrapper::ZerobusSdk>()?;
    sync_module.add_class::<sync_wrapper::ZerobusStream>()?;
    sync_module.add_class::<sync_wrapper::RecordAcknowledgment>()?;
    m.add_submodule(sync_module)?;

    // Add aio (async) submodule
    let aio_module = PyModule::new(py, "aio")?;
    aio_module.add_class::<async_wrapper::ZerobusSdk>()?;
    aio_module.add_class::<async_wrapper::ZerobusStream>()?;
    aio_module.add_class::<async_wrapper::PyAckFuture>()?;
    m.add_submodule(aio_module)?;

    Ok(())
}
