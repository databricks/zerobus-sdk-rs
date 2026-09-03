use std::env;

#[cfg(feature = "zeroparser")]
#[path = "src/zeroparser/proto_build.rs"]
mod zeroparser_proto_build;

fn main() {
    if env::var_os("PROTOC").is_none() {
        if let Ok(protoc) = protoc_bin_vendored::protoc_bin_path() {
            unsafe {
                env::set_var("PROTOC", protoc);
            }
        }
    }
    tonic_prost_build::compile_protos("zerobus_service.proto")
        .unwrap_or_else(|e| panic!("Failed to compile protos {:?}", e));

    #[cfg(feature = "zeroparser")]
    zeroparser_proto_build::compile();
}
