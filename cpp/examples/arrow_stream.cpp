// Ingest Arrow RecordBatches over an Arrow Flight stream (Beta).
//
// Records cross the FFI as Arrow IPC stream bytes. Producing those bytes
// requires the Arrow C++ library, which this SDK does not depend on, so this
// example reads pre-encoded IPC bytes from files:
//   ZEROBUS_ARROW_SCHEMA_IPC   file with an IPC stream encoding only the schema
//   ZEROBUS_ARROW_BATCH_IPC    file with an IPC stream (schema + one batch)
// See json_single.cpp for the connection environment variables.

#include <cstdlib>
#include <fstream>
#include <iostream>
#include <iterator>
#include <string>
#include <vector>

#include "zerobus/zerobus.hpp"

namespace {
std::string env(const char* name) {
  const char* v = std::getenv(name);
  return v != nullptr ? std::string(v) : std::string();
}

std::vector<std::uint8_t> read_file(const std::string& path) {
  std::ifstream in(path, std::ios::binary);
  if (!in) {
    throw std::runtime_error("cannot open " + path);
  }
  return std::vector<std::uint8_t>(std::istreambuf_iterator<char>(in),
                                   std::istreambuf_iterator<char>());
}
}  // namespace

int main() {
  try {
    std::vector<std::uint8_t> schema_ipc =
        read_file(env("ZEROBUS_ARROW_SCHEMA_IPC"));
    std::vector<std::uint8_t> batch_ipc =
        read_file(env("ZEROBUS_ARROW_BATCH_IPC"));

    zerobus::Sdk sdk = zerobus::Sdk::builder()
                           .endpoint(env("ZEROBUS_SERVER_ENDPOINT"))
                           .unity_catalog_url(env("DATABRICKS_WORKSPACE_URL"))
                           .application_name("zerobus-cpp-arrow-example")
                           .build();

    zerobus::ArrowStreamOptions options;
    options.ipc_compression = zerobus::IpcCompression::Zstd;

    zerobus::ArrowStream stream = sdk.create_arrow_stream(
        env("ZEROBUS_TABLE_NAME"), schema_ipc, env("DATABRICKS_CLIENT_ID"),
        env("DATABRICKS_CLIENT_SECRET"), options);

    std::int64_t offset = stream.ingest_batch(batch_ipc);
    std::cout << "ingested batch at offset " << offset << "\n";

    stream.flush();
    stream.close();
    std::cout << "done\n";
    return 0;
  } catch (const zerobus::ZerobusException& e) {
    std::cerr << "zerobus error: " << e.what()
              << " (retryable=" << (e.is_retryable() ? "true" : "false")
              << ")\n";
    return 1;
  } catch (const std::exception& e) {
    std::cerr << "error: " << e.what() << "\n";
    return 1;
  }
}
