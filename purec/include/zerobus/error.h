/*
 * Zerobus Pure C SDK — error objects.
 */
#ifndef ZEROBUS_ERROR_H
#define ZEROBUS_ERROR_H

#include "zerobus/common.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Failable functions take an optional `zerobus_error_t **out_error`. On
 * failure it may be populated with an owned error, which the caller must free
 * with zerobus_error_free. Passing NULL discards the error. */

/* The returned view is borrowed from the error, valid only until it is freed.
 */
ZEROBUS_API zerobus_string_view_t ZEROBUS_CALL
zerobus_error_message(const zerobus_error_t *error);

ZEROBUS_API void ZEROBUS_CALL zerobus_error_free(zerobus_error_t *error);

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* ZEROBUS_ERROR_H */
