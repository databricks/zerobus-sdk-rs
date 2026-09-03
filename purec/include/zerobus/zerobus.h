/*
 * Zerobus Pure C SDK — public API umbrella header.
 *
 * Includes the whole public surface. Per-area headers can be included directly:
 * common.h, error.h, sdk.h, stream.h.
 *
 * The SDK is under early development: the functions validate inputs and honor
 * the documented status/ownership rules, but the networking core is not
 * implemented yet. See README.md.
 */
#ifndef ZEROBUS_H
#define ZEROBUS_H

#include "zerobus/common.h" // IWYU pragma: export
#include "zerobus/error.h"  // IWYU pragma: export
#include "zerobus/sdk.h"    // IWYU pragma: export
#include "zerobus/stream.h" // IWYU pragma: export

#endif /* ZEROBUS_H */
