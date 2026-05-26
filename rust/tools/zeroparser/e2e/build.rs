use std::env;
use std::path::PathBuf;

fn main() {
    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR not set"));

    prost_build::Config::new()
        .out_dir(&out_dir)
        .file_descriptor_set_path(out_dir.join("e2e_descriptor_set.bin"))
        .compile_protos(
            &[
                "tests/proto/test_proto2.proto",
                "tests/proto/test_proto3.proto",
            ],
            &["tests/proto"],
        )
        .expect("failed to compile e2e protos");

    prost_build::Config::new()
        .file_descriptor_set_path(out_dir.join("bench_descriptor_set.bin"))
        .compile_protos(
            &[
                "benches/proto/air_quality.proto",
                "benches/proto/click_bench.proto",
                "benches/proto/supported_nullable_types.proto",
            ],
            &["benches/proto"],
        )
        .expect("failed to compile bench protos");

    println!("cargo:rerun-if-changed=tests/proto");
    println!("cargo:rerun-if-changed=benches/proto");
}
