fn main() -> Result<(), std::io::Error> {
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(false)
        .compile_protos(&["../sdk/zerobus_service.proto"], &["../sdk"])
}
