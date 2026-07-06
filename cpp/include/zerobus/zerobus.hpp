#ifndef ZEROBUS_HPP
#define ZEROBUS_HPP

/// Umbrella header for the Zerobus C++ SDK. Include this to pull in the full
/// public API.
///
/// The SDK is a thin, RAII C++ wrapper over the Zerobus C FFI (`zerobus.h`),
/// which in turn wraps the Rust core. All operations report failure by throwing
/// `zerobus::ZerobusException`.

#include "zerobus/config.hpp"
#include "zerobus/error.hpp"
#include "zerobus/headers_provider.hpp"
#include "zerobus/proto_schema.hpp"
#include "zerobus/record.hpp"
#include "zerobus/sdk.hpp"
#include "zerobus/stream.hpp"
#include "zerobus/version.hpp"

#endif  // ZEROBUS_HPP
