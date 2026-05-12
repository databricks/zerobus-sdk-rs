fn main() -> Result<(), Box<dyn std::error::Error>> {
    std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path().unwrap());
    tonic_build::configure()
        .build_server(true)
        .build_client(false)
        .compile_protos(&["../sdk/zerobus_service.proto"], &["../sdk"])?;

    Ok(())
}
