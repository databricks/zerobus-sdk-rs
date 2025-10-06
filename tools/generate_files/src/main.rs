use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
// When building with Cargo, use the simple crate name
// #[cfg(not(feature = "bazel"))]
// use tools::{clean_filename, fetch_table_info, generate_proto_file, generate_rust_and_descriptor};

// // When building with Bazel, use the full crate name
// #[cfg(feature = "bazel")]
// use crate::{clean_filename, fetch_table_info, generate_proto_file, generate_rust_and_descriptor};

mod generate;
use generate::{
    clean_filename, fetch_table_info, generate_proto_file, generate_rust_and_descriptor,
};

/// CLI arguments.
#[derive(Parser, Debug)]
#[command(about = "Generate proto files from UC table schemas")]
struct Args {
    /// Unity Catalog endpoint URL (e.g., https://your-workspace.cloud.databricks.com)
    #[arg(long = "uc-endpoint")]
    uc_endpoint: String,

    /// Unity Catalog authentication token (e.g., dapi123...)
    #[arg(long = "uc-token")]
    uc_token: String,

    /// Full table name in format: catalog.schema.table_name
    #[arg(long = "table")]
    table: String,

    /// Output path for the generated proto file (e.g., output.proto)
    #[arg(long = "output")]
    output: Option<PathBuf>,

    /// Name of the protobuf message (defaults to table name)
    #[arg(long = "proto-msg")]
    proto_msg: Option<String>,

    /// Output directory for generated files (defaults to "output")
    #[arg(long = "output-dir", default_value = "output")]
    output_dir: PathBuf,
}

fn main() -> Result<()> {
    let args = Args::parse();

    let table_info = fetch_table_info(&args.uc_endpoint, &args.uc_token, &args.table)
        .context("UC request failed")?;

    let msg_name = args.proto_msg.unwrap_or_else(|| {
        args.table
            .split('.')
            .next_back()
            .unwrap_or("TableMessage")
            .to_string()
    });

    let output = args.output.unwrap_or_else(|| {
        let table_name = args.table.split('.').next_back().unwrap_or("table");
        let clean_name = clean_filename(table_name);
        PathBuf::from(format!("{}.proto", clean_name))
    });

    let proto_output_path = args.output_dir.join(&output);

    generate_proto_file(
        &msg_name,
        &table_info.columns,
        &proto_output_path,
        &args.output_dir,
    )
    .context("failed to write proto file")?;

    generate_rust_and_descriptor(
        proto_output_path.to_str().unwrap(),
        &msg_name,
        &args.output_dir,
    )?;

    Ok(())
}
