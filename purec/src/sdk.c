#include <stdlib.h>
#include <string.h>

#include "error.h"
#include "utils.h"
#include "zerobus/sdk.h"

struct zerobus_sdk_builder {
    char *endpoint;    /* zerobus endpoint, owned */
    char *uc_endpoint; /* unity catalog endpoint, owned */
};

struct zerobus_sdk {
    /* Independent copies of the validated configuration. */
    char *endpoint;
    char *uc_endpoint;
};

/* ---- builder ----------------------------------------------------------- */

zerobus_status_t zerobus_sdk_builder_new(zerobus_sdk_builder_t **out_builder,
                                         zerobus_error_t **out_error)
{
    if (out_error != NULL) {
        *out_error = NULL;
    }
    if (out_builder == NULL) {
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "out_builder must not be NULL");
    }
    *out_builder = NULL;
    zerobus_sdk_builder_t *b = (zerobus_sdk_builder_t *)calloc(1, sizeof(*b));
    if (b == NULL) {
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    *out_builder = b;
    return ZEROBUS_STATUS_OK;
}

static zerobus_status_t validate_endpoint(const char *endpoint,
                                          const char *label, char **out_host,
                                          zerobus_error_t **out_error)
{
    bool is_https = false;
    switch (zb_url_validate(endpoint, out_host, &is_https)) {
    case ZB_URL_OK:
        break;
    case ZB_URL_NO_SCHEME:
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "%s must start with https://: %s", label, endpoint);
    case ZB_URL_EMPTY_HOST:
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "%s has no host: %s", label, endpoint);
    case ZB_URL_BAD_HOST:
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "%s host is malformed: %s", label, endpoint);
    case ZB_URL_BAD_PORT:
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "%s has an invalid port: %s", label, endpoint);
    case ZB_URL_OOM:
    default:
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    if (!is_https) {
        if (out_host != NULL) {
            free(*out_host);
            *out_host = NULL;
        }
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "%s must use https: %s", label, endpoint);
    }
    return ZEROBUS_STATUS_OK;
}

zerobus_status_t
zerobus_sdk_builder_set_endpoint(zerobus_sdk_builder_t *builder,
                                 zerobus_string_view_t zerobus_endpoint,
                                 zerobus_error_t **out_error)
{
    if (out_error != NULL) {
        *out_error = NULL;
    }
    if (builder == NULL) {
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "builder must not be NULL");
    }
    if (!zb_is_valid_string(zerobus_endpoint)) {
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "zerobus endpoint must be non-empty valid UTF-8");
    }
    /* Validate before storing (transactional): a rejected endpoint leaves the
     * previous value in place. */
    char *copy = zb_strdup_view(zerobus_endpoint);
    if (copy == NULL) {
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    char *host = NULL;
    zerobus_status_t s =
        validate_endpoint(copy, "zerobus endpoint", &host, out_error);
    if (s != ZEROBUS_STATUS_OK) {
        free(copy);
        return s;
    }
    /* Zerobus-only: require a dot with a label after it, so a single-label host
     * ("localhost", "localhost.") is rejected. */
    const char *dot = strchr(host, '.');
    if (dot == NULL || dot[1] == '\0') {
        zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                "zerobus endpoint host \"%s\" has no workspace subdomain",
                host);
        free(host);
        free(copy);
        return ZEROBUS_STATUS_INVALID_ARGUMENT;
    }
    free(host);
    free(builder->endpoint);
    builder->endpoint = copy;
    return ZEROBUS_STATUS_OK;
}

zerobus_status_t zerobus_sdk_builder_set_unity_catalog_endpoint(
    zerobus_sdk_builder_t *builder,
    zerobus_string_view_t unity_catalog_endpoint, zerobus_error_t **out_error)
{
    if (out_error != NULL) {
        *out_error = NULL;
    }
    if (builder == NULL) {
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "builder must not be NULL");
    }
    if (!zb_is_valid_string(unity_catalog_endpoint)) {
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "unity catalog endpoint must be non-empty valid UTF-8");
    }
    char *copy = zb_strdup_view(unity_catalog_endpoint);
    if (copy == NULL) {
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    /* Host presence is enough for the UC endpoint; no subdomain requirement. */
    zerobus_status_t s =
        validate_endpoint(copy, "unity catalog endpoint", NULL, out_error);
    if (s != ZEROBUS_STATUS_OK) {
        free(copy);
        return s;
    }
    free(builder->uc_endpoint);
    builder->uc_endpoint = copy;
    return ZEROBUS_STATUS_OK;
}

zerobus_status_t zerobus_sdk_builder_build(const zerobus_sdk_builder_t *builder,
                                           zerobus_sdk_t **out_sdk,
                                           zerobus_error_t **out_error)
{
    if (out_error != NULL) {
        *out_error = NULL;
    }
    if (out_sdk != NULL) {
        *out_sdk = NULL;
    }
    if (builder == NULL || out_sdk == NULL) {
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "builder and out_sdk must not be NULL");
    }
    if (builder->endpoint == NULL) {
        return zb_fail(out_error, ZEROBUS_STATUS_FAILED_PRECONDITION,
                       "zerobus endpoint is required");
    }
    if (builder->uc_endpoint == NULL) {
        return zb_fail(out_error, ZEROBUS_STATUS_FAILED_PRECONDITION,
                       "unity catalog endpoint is required");
    }

    /* Endpoints were validated when set.
     * TODO: initialize shared TLS/OAuth/transport state. */
    zerobus_sdk_t *sdk = (zerobus_sdk_t *)calloc(1, sizeof(*sdk));
    if (sdk == NULL) {
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    sdk->endpoint = zb_strdup(builder->endpoint);
    sdk->uc_endpoint = zb_strdup(builder->uc_endpoint);
    if (sdk->endpoint == NULL || sdk->uc_endpoint == NULL) {
        zerobus_sdk_free(sdk);
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }

    *out_sdk = sdk;
    return ZEROBUS_STATUS_OK;
}

void zerobus_sdk_builder_free(zerobus_sdk_builder_t *builder)
{
    if (builder == NULL) {
        return;
    }
    free(builder->endpoint);
    free(builder->uc_endpoint);
    free(builder);
}

/* ---- SDK --------------------------------------------------------------- */

void zerobus_sdk_free(zerobus_sdk_t *sdk)
{
    if (sdk == NULL) {
        return;
    }
    /* TODO: tear down shared transport/auth resources (best-effort). */
    free(sdk->endpoint);
    free(sdk->uc_endpoint);
    free(sdk);
}
