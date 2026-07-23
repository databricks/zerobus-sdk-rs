#ifndef ZEROBUS_VERSION_HPP
#define ZEROBUS_VERSION_HPP

#define ZEROBUS_CPP_VERSION_MAJOR 0
#define ZEROBUS_CPP_VERSION_MINOR 1
#define ZEROBUS_CPP_VERSION_PATCH 1
#define ZEROBUS_CPP_VERSION "0.1.1"

namespace zerobus {

/// The Zerobus C++ SDK version string (e.g. "0.1.0"). Sent as the default
/// `user-agent` SDK identifier prefix (`zerobus-sdk-cpp/<version>`).
inline const char* version() { return ZEROBUS_CPP_VERSION; }

}  // namespace zerobus

#endif  // ZEROBUS_VERSION_HPP
