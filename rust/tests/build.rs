fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::var_os("PROTOC").is_none() {
        if let Ok(protoc) = protoc_bin_vendored::protoc_bin_path() {
            unsafe {
                std::env::set_var("PROTOC", protoc);
            }
        }
    }
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(false)
        .compile_protos(&["../sdk/zerobus_service.proto"], &["../sdk"])?;

    Ok(())
}
