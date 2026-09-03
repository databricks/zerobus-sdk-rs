#include <stddef.h>
#include <stdlib.h>

#include "error.h"
#include "utils.h"
#include "zerobus/stream.h"

/* Record size limit: just under the server's ~10 MiB request cap, leaving
 * headroom for framing and protocol overhead. No public knob for it yet. */
#define ZB_MAX_PAYLOAD_BYTES (10u * 1024u * 1024u - 64u * 1024u)

struct zerobus_stream_builder {
    zerobus_sdk_t *sdk; /* borrowed, must outlive the builder and its streams */
    char *table;
    char *client_id;
    char *client_secret; /* zeroized on free */
};

struct zerobus_stream {
    zerobus_sdk_t *sdk; /* borrowed, must outlive the stream */
    char *table;
    char *client_id;
    char *client_secret; /* zeroized on free */

    bool closed; /* set by close, rejects ingest once true */
};

/* ---- builder ----------------------------------------------------------- */

zerobus_status_t
zerobus_stream_builder_new(zerobus_sdk_t *sdk,
                           zerobus_stream_builder_t **out_builder,
                           zerobus_error_t **out_error)
{
    if (out_error != NULL) {
        *out_error = NULL;
    }
    if (out_builder != NULL) {
        *out_builder = NULL;
    }
    if (sdk == NULL || out_builder == NULL) {
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "sdk and out_builder must not be NULL");
    }
    zerobus_stream_builder_t *b =
        (zerobus_stream_builder_t *)calloc(1, sizeof(*b));
    if (b == NULL) {
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    b->sdk = sdk;
    *out_builder = b;
    return ZEROBUS_STATUS_OK;
}

zerobus_status_t
zerobus_stream_builder_set_table(zerobus_stream_builder_t *builder,
                                 zerobus_string_view_t table_name,
                                 zerobus_error_t **out_error)
{
    if (out_error != NULL) {
        *out_error = NULL;
    }
    if (builder == NULL) {
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "builder must not be NULL");
    }
    if (!zb_is_valid_string(table_name)) {
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "table name must be non-empty valid UTF-8");
    }
    if (!zb_table_name_is_valid(table_name)) {
        return zb_fail(
            out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
            "table name must be fully qualified as catalog.schema.table");
    }
    if (!zb_replace_string(&builder->table, table_name)) {
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    return ZEROBUS_STATUS_OK;
}

zerobus_status_t zerobus_stream_builder_set_oauth(
    zerobus_stream_builder_t *builder, zerobus_string_view_t client_id,
    zerobus_string_view_t client_secret, zerobus_error_t **out_error)
{
    if (out_error != NULL) {
        *out_error = NULL;
    }
    if (builder == NULL) {
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "builder must not be NULL");
    }
    if (!zb_is_valid_string(client_id) || !zb_is_valid_string(client_secret)) {
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "client id and secret must be non-empty valid UTF-8");
    }
    /* Allocate both copies before mutating the builder (transactional). */
    char *id_copy = zb_strdup_view(client_id);
    char *secret_copy = zb_strdup_view(client_secret);
    if (id_copy == NULL || secret_copy == NULL) {
        free(id_copy);
        zb_secure_free_cstr(secret_copy);
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    free(builder->client_id);
    zb_secure_free_cstr(builder->client_secret);
    builder->client_id = id_copy;
    builder->client_secret = secret_copy;
    return ZEROBUS_STATUS_OK;
}

void zerobus_stream_builder_free(zerobus_stream_builder_t *builder)
{
    if (builder == NULL) {
        return;
    }
    free(builder->table);
    free(builder->client_id);
    zb_secure_free_cstr(builder->client_secret);
    free(builder);
}

zerobus_status_t
zerobus_stream_builder_build(const zerobus_stream_builder_t *builder,
                             zerobus_stream_t **out_stream,
                             zerobus_error_t **out_error)
{
    if (out_error != NULL) {
        *out_error = NULL;
    }
    if (out_stream != NULL) {
        *out_stream = NULL;
    }
    if (builder == NULL || out_stream == NULL) {
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "builder and out_stream must not be NULL");
    }
    if (builder->table == NULL) {
        return zb_fail(out_error, ZEROBUS_STATUS_FAILED_PRECONDITION,
                       "table name is required");
    }
    if (builder->client_id == NULL || builder->client_secret == NULL) {
        return zb_fail(out_error, ZEROBUS_STATUS_FAILED_PRECONDITION,
                       "OAuth client credentials are required");
    }

    /* TODO: mint an OAuth token and open the authenticated stream. */
    zerobus_stream_t *s = (zerobus_stream_t *)calloc(1, sizeof(*s));
    if (s == NULL) {
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }
    s->sdk = builder->sdk;
    s->table = zb_strdup(builder->table);
    s->client_id = zb_strdup(builder->client_id);
    s->client_secret = zb_strdup(builder->client_secret);
    if (s->table == NULL || s->client_id == NULL || s->client_secret == NULL) {
        zerobus_stream_free(s);
        return ZEROBUS_STATUS_OUT_OF_MEMORY;
    }

    *out_stream = s;
    return ZEROBUS_STATUS_OK;
}

/* ---- ingest ------------------------------------------------------------ */

zerobus_status_t
zerobus_stream_ingest_json_record(zerobus_stream_t *stream,
                                  zerobus_string_view_t json_record,
                                  zerobus_error_t **out_error)
{
    if (out_error != NULL) {
        *out_error = NULL;
    }
    if (stream == NULL) {
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "stream must not be NULL");
    }
    if (stream->closed) {
        return zb_fail(out_error, ZEROBUS_STATUS_FAILED_PRECONDITION,
                       "stream is closing or closed");
    }
    if (zb_is_empty(json_record)) {
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "record must be a non-empty JSON value");
    }
    if (json_record.len > ZB_MAX_PAYLOAD_BYTES) {
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "record of %zu bytes exceeds the %u-byte limit",
                       json_record.len, (unsigned)ZB_MAX_PAYLOAD_BYTES);
    }
    if (!zb_is_valid_json(json_record)) {
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "record is not a well-formed UTF-8 JSON value");
    }

    /* TODO: copy the record, assign its sequence, and queue it for the sender.
     */
    return ZEROBUS_STATUS_OK;
}

/* ---- flush ------------------------------------------------------------- */

zerobus_status_t zerobus_stream_flush(zerobus_stream_t *stream,
                                      zerobus_error_t **out_error)
{
    if (out_error != NULL) {
        *out_error = NULL;
    }
    if (stream == NULL) {
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "stream must not be NULL");
    }
    /* TODO: wait for the server to acknowledge every queued record. */
    return ZEROBUS_STATUS_OK;
}

/* ---- close ------------------------------------------------------------- */

zerobus_status_t zerobus_stream_close(zerobus_stream_t *stream,
                                      zerobus_error_t **out_error)
{
    if (out_error != NULL) {
        *out_error = NULL;
    }
    if (stream == NULL) {
        return zb_fail(out_error, ZEROBUS_STATUS_INVALID_ARGUMENT,
                       "stream must not be NULL");
    }
    /* TODO: flush pending records before teardown, returning any flush error.
     */
    stream->closed = true;
    return ZEROBUS_STATUS_OK;
}

/* ---- free -------------------------------------------------------------- */

void zerobus_stream_free(zerobus_stream_t *stream)
{
    if (stream == NULL) {
        return;
    }
    /* TODO: tear down the transport and join I/O workers before freeing. */
    free(stream->table);
    free(stream->client_id);
    zb_secure_free_cstr(stream->client_secret);
    free(stream);
}
