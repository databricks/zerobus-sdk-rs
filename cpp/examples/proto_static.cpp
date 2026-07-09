// Zerobus C++ SDK — proto ingestion with a STATIC (checked-in) schema.
//
// The counterpart to proto_ingest.cpp. Instead of fetching the table's
// descriptor from Unity Catalog at runtime (ProtoSchema::from_uc_json), this
// example uses a hand-written air_quality.proto compiled by protoc. That trades
// away drift-safety (the .proto must be kept in sync with the table by hand)
// for two things: it needs NO Unity Catalog workspace token / curl, and record
// fields are set with compile-time type safety.
//
// Two pieces replace the dynamic ProtoSchema:
//   * descriptor bytes  — AirQuality::descriptor()->CopyTo(&DescriptorProto),
//     serialized, as TableProperties::descriptor_proto. NOTE: the core wants a
//     single-message DescriptorProto, not a FileDescriptorProto.
//   * record encoding    — set fields on an AirQuality message and
//     SerializeToString(), instead of ProtoSchema::encode_json().
//
// Non-secret connection info lives in demo_config.hpp. Only the OAuth client
// credentials come from the environment — no workspace token needed:
//
//   export ZEROBUS_CLIENT_ID="<oauth-client-id>"
//   export ZEROBUS_CLIENT_SECRET="<oauth-client-secret>"
//
//   ./build/examples/proto_static
//
// The .proto's field numbers (1,2,3) must match the table's column ordinals
// (device_name, temp, humidity).

#include <google/protobuf/descriptor.pb.h>

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "air_quality.pb.h"
#include "demo_config.hpp"
#include "zerobus/zerobus.hpp"

namespace {

std::string require_env(const char* name) {
  const char* value = std::getenv(name);
  if (value == nullptr || *value == '\0') {
    std::cerr << "error: environment variable " << name << " is not set.\n"
              << "See the header of this file for the required variables.\n";
    std::exit(2);
  }
  return value;
}

// Serialize the DescriptorProto (single message) for the generated AirQuality
// message. This is what dynamic ProtoSchema::descriptor_bytes() returned; here
// we build it from the compiled-in descriptor instead.
std::vector<std::uint8_t> air_quality_descriptor_bytes() {
  google::protobuf::DescriptorProto descriptor;
  zerobus_demo::AirQuality::descriptor()->CopyTo(&descriptor);
  std::string serialized;
  descriptor.SerializeToString(&serialized);
  return std::vector<std::uint8_t>(serialized.begin(), serialized.end());
}

// Encode one reading to proto wire bytes for tick `n`, varying the values so
// the rows differ (mirrors demo_config::make_record but typed).
std::vector<std::uint8_t> encode_record(int n) {
  zerobus_demo::AirQuality msg;
  msg.set_device_name("device_num_1");
  msg.set_temp(20 + (n % 15));
  msg.set_humidity(40 + (n % 40));

  std::string serialized;
  msg.SerializeToString(&serialized);
  return std::vector<std::uint8_t>(serialized.begin(), serialized.end());
}

}  // namespace

int main() {
  GOOGLE_PROTOBUF_VERIFY_VERSION;

  // Only the OAuth client credentials come from the environment.
  const std::string client_id = require_env("ZEROBUS_CLIENT_ID");
  const std::string client_secret = require_env("ZEROBUS_CLIENT_SECRET");
  const std::string endpoint = zerobus_demo::kZerobusEndpoint;
  const std::string uc_url = zerobus_demo::kWorkspaceUrl;
  const std::string table = zerobus_demo::table_name();

  try {
    // 1. Build the SDK.
    zerobus::Sdk sdk = zerobus::Sdk::builder()
                           .endpoint(endpoint)
                           .unity_catalog_url(uc_url)
                           .application_name("proto-static")
                           .build();

    // 2. Open a proto stream using the STATIC descriptor built from the
    //    compiled-in .proto — no Unity Catalog fetch.
    zerobus::TableProperties props;
    props.table_name = table;
    props.descriptor_proto = air_quality_descriptor_bytes();

    zerobus::StreamOptions options;
    options.record_type = zerobus::RecordType::Proto;

    zerobus::Stream stream =
        sdk.create_stream(props, client_id, client_secret, options);

    // 3. Stream continuously: emit one reading per tick for the configured
    //    duration, with NO per-record wait. flush() only every N records.
    using clock = std::chrono::steady_clock;
    const auto deadline = clock::now() + std::chrono::milliseconds(
                                             zerobus_demo::kStreamDurationMs);
    const auto tick = std::chrono::milliseconds(zerobus_demo::kTickIntervalMs);

    int sent = 0;
    std::int64_t last_offset = -1;
    while (clock::now() < deadline) {
      last_offset = stream.ingest_proto_record(encode_record(sent));
      ++sent;

      if (sent % zerobus_demo::kFlushEveryNRecords == 0) {
        stream.flush();
        std::cout << "... sent " << sent << " (last offset " << last_offset
                  << ")\n";
      }

      std::this_thread::sleep_for(tick);
    }

    // 4. Final flush + close.
    stream.flush();
    stream.close();

    std::cout << "Done. sent " << sent << " records; last offset "
              << last_offset << "\n";
  } catch (const zerobus::ZerobusException& e) {
    std::cerr << "Zerobus error: " << e.what()
              << " (retryable=" << (e.is_retryable() ? "true" : "false")
              << ")\n";
    return 1;
  }

  // Optional: release protobuf's process-wide state so leak checkers stay
  // quiet.
  google::protobuf::ShutdownProtobufLibrary();
  return 0;
}
