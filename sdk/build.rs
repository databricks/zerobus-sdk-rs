fn main() {
    std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path().unwrap());
    tonic_build::compile_protos("zerobus_service.proto")
        .unwrap_or_else(|e| panic!("Failed to compile protos {:?}", e));
}
