/* Unit tests for the stream builder and stream handle (stream.c). */
#include <stdlib.h>
#include <string.h>

#include "test_common.h"

/* ---- helpers ----------------------------------------------------------- */

/* Build a valid SDK for the stream tests. Caller frees it. */
static zerobus_sdk_t *make_sdk(void)
{
    zerobus_sdk_builder_t *b = NULL;
    zerobus_sdk_builder_new(&b, NULL);
    zerobus_sdk_builder_set_endpoint(
        b, sv("https://myws.zerobus.us-west.cloud.databricks.com"), NULL);
    zerobus_sdk_builder_set_unity_catalog_endpoint(
        b, sv("https://myws.cloud.databricks.com"), NULL);
    zerobus_sdk_t *sdk = NULL;
    zerobus_sdk_builder_build(b, &sdk, NULL);
    zerobus_sdk_builder_free(b);
    return sdk;
}

/* Build a valid stream for the ingest/flush/close tests. Caller frees the
 * stream and the SDK passed in. */
static zerobus_stream_t *make_stream(zerobus_sdk_t *sdk)
{
    zerobus_stream_builder_t *stb = NULL;
    zerobus_stream_builder_new(sdk, &stb, NULL);
    zerobus_stream_builder_set_table(stb, sv("cat.sch.tbl"), NULL);
    zerobus_stream_builder_set_oauth(stb, sv("client-id"), sv("client-secret"),
                                     NULL);
    zerobus_stream_t *stream = NULL;
    zerobus_stream_builder_build(stb, &stream, NULL);
    zerobus_stream_builder_free(stb);
    return stream;
}

/* ---- tests ------------------------------------------------------------- */

static void test_stream_builder_validation(void)
{
    zerobus_sdk_t *sdk = make_sdk();
    CHECK(sdk != NULL);

    zerobus_error_t *err = NULL;

    /* NULL sdk. */
    zerobus_stream_builder_t *stb = NULL;
    CHECK_EQ_INT(zerobus_stream_builder_new(NULL, &stb, &err),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    zerobus_error_free(err);
    err = NULL;

    CHECK_EQ_INT(zerobus_stream_builder_new(sdk, &stb, &err),
                 ZEROBUS_STATUS_OK);
    CHECK(stb != NULL);

    /* Empty table. */
    CHECK_EQ_INT(zerobus_stream_builder_set_table(
                     stb, (zerobus_string_view_t){NULL, 0}, &err),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    zerobus_error_free(err);
    err = NULL;

    /* Not fully qualified (catalog.schema.table). */
    CHECK_EQ_INT(
        zerobus_stream_builder_set_table(stb, sv("schema.table"), &err),
        ZEROBUS_STATUS_INVALID_ARGUMENT);
    zerobus_error_free(err);
    err = NULL;

    /* Empty credentials. */
    CHECK_EQ_INT(
        zerobus_stream_builder_set_oauth(stb, sv(""), sv("secret"), &err),
        ZEROBUS_STATUS_INVALID_ARGUMENT);
    zerobus_error_free(err);
    err = NULL;

    /* Build without a table set: fails before any network work. */
    zerobus_stream_t *stream = NULL;
    CHECK_EQ_INT(zerobus_stream_builder_build(stb, &stream, &err),
                 ZEROBUS_STATUS_FAILED_PRECONDITION);
    CHECK(stream == NULL);
    zerobus_error_free(err);
    err = NULL;

    /* Table set but no credentials: still fails the precondition. */
    zerobus_stream_builder_set_table(stb, sv("cat.sch.tbl"), NULL);
    CHECK_EQ_INT(zerobus_stream_builder_build(stb, &stream, &err),
                 ZEROBUS_STATUS_FAILED_PRECONDITION);
    zerobus_error_free(err);
    err = NULL;

    zerobus_stream_builder_free(stb);
    zerobus_sdk_free(sdk);
}

/* NULL-builder setters and NULL out_stream. */
static void test_stream_builder_edges(void)
{
    zerobus_sdk_t *sdk = make_sdk();
    zerobus_error_t *err = NULL;

    CHECK_EQ_INT(zerobus_stream_builder_set_table(NULL, sv("c.s.t"), &err),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    zerobus_error_free(err);
    err = NULL;
    CHECK_EQ_INT(
        zerobus_stream_builder_set_oauth(NULL, sv("id"), sv("secret"), &err),
        ZEROBUS_STATUS_INVALID_ARGUMENT);
    zerobus_error_free(err);
    err = NULL;

    zerobus_stream_builder_t *stb = NULL;
    zerobus_stream_builder_new(sdk, &stb, NULL);

    /* NULL out_stream. */
    zerobus_stream_builder_set_table(stb, sv("c.s.t"), NULL);
    CHECK_EQ_INT(zerobus_stream_builder_build(stb, NULL, &err),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    zerobus_error_free(err);

    zerobus_stream_builder_free(stb);
    zerobus_sdk_free(sdk);
}

/* set_oauth is transactional: a second valid call replaces (and frees) the
 * previous credentials, and a rejected call leaves them in place. */
static void test_stream_oauth_transactional(void)
{
    zerobus_sdk_t *sdk = make_sdk();
    zerobus_stream_builder_t *stb = NULL;
    zerobus_stream_builder_new(sdk, &stb, NULL);
    zerobus_stream_builder_set_table(stb, sv("cat.sch.tbl"), NULL);

    /* A second valid set_oauth replaces (and frees) the first. */
    CHECK_EQ_INT(
        zerobus_stream_builder_set_oauth(stb, sv("id1"), sv("sec1"), NULL),
        ZEROBUS_STATUS_OK);
    CHECK_EQ_INT(
        zerobus_stream_builder_set_oauth(stb, sv("id2"), sv("sec2"), NULL),
        ZEROBUS_STATUS_OK);

    /* A rejected set_oauth must not clobber the credentials already set. */
    CHECK_EQ_INT(
        zerobus_stream_builder_set_oauth(stb, sv(""), sv("sec3"), NULL),
        ZEROBUS_STATUS_INVALID_ARGUMENT);

    /* Build still succeeds using the credentials set before the failed call. */
    zerobus_stream_t *stream = NULL;
    CHECK_EQ_INT(zerobus_stream_builder_build(stb, &stream, NULL),
                 ZEROBUS_STATUS_OK);
    CHECK(stream != NULL);

    zerobus_stream_free(stream);
    zerobus_stream_builder_free(stb);
    zerobus_sdk_free(sdk);
}

static void test_ingest_validation(void)
{
    zerobus_sdk_t *sdk = make_sdk();
    zerobus_stream_t *stream = make_stream(sdk);
    CHECK(stream != NULL);

    zerobus_error_t *err = NULL;

    /* NULL stream. */
    CHECK_EQ_INT(zerobus_stream_ingest_json_record(NULL, sv("{}"), &err),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    zerobus_error_free(err);
    err = NULL;

    /* Empty record. */
    CHECK_EQ_INT(zerobus_stream_ingest_json_record(
                     stream, (zerobus_string_view_t){NULL, 0}, &err),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    zerobus_error_free(err);
    err = NULL;

    /* Malformed JSON. */
    CHECK_EQ_INT(
        zerobus_stream_ingest_json_record(stream, sv("{not json"), &err),
        ZEROBUS_STATUS_INVALID_ARGUMENT);
    zerobus_error_free(err);
    err = NULL;

    /* Well-formed JSON is admitted. */
    CHECK_EQ_INT(zerobus_stream_ingest_json_record(
                     stream, sv("{\"id\":1,\"m\":\"hi\"}"), &err),
                 ZEROBUS_STATUS_OK);
    CHECK(err == NULL);

    /* A record past the size ceiling is refused before JSON parsing. */
    size_t big = 11u * 1024u * 1024u;
    char *buf = (char *)malloc(big);
    CHECK(buf != NULL);
    if (buf != NULL) {
        memset(buf, 'a', big);
        CHECK_EQ_INT(zerobus_stream_ingest_json_record(
                         stream, (zerobus_string_view_t){buf, big}, &err),
                     ZEROBUS_STATUS_INVALID_ARGUMENT);
        zerobus_error_free(err);
        free(buf);
    }

    zerobus_stream_free(stream);
    zerobus_sdk_free(sdk);
}

static void test_flush_close_idempotent(void)
{
    zerobus_sdk_t *sdk = make_sdk();
    zerobus_stream_t *stream = make_stream(sdk);
    CHECK(stream != NULL);

    zerobus_error_t *err = NULL;

    /* NULL stream. */
    CHECK_EQ_INT(zerobus_stream_flush(NULL, &err),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    zerobus_error_free(err);
    err = NULL;
    CHECK_EQ_INT(zerobus_stream_close(NULL, &err),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    zerobus_error_free(err);
    err = NULL;

    CHECK_EQ_INT(zerobus_stream_flush(stream, &err), ZEROBUS_STATUS_OK);
    CHECK_EQ_INT(zerobus_stream_close(stream, &err), ZEROBUS_STATUS_OK);
    /* Idempotent: a second close still succeeds. */
    CHECK_EQ_INT(zerobus_stream_close(stream, &err), ZEROBUS_STATUS_OK);
    CHECK(err == NULL);

    /* After close, ingest is rejected with FAILED_PRECONDITION. */
    CHECK_EQ_INT(
        zerobus_stream_ingest_json_record(stream, sv("{\"id\":2}"), &err),
        ZEROBUS_STATUS_FAILED_PRECONDITION);
    zerobus_error_free(err);
    err = NULL;

    /* Even a malformed record: the closed check precedes record validation. */
    CHECK_EQ_INT(
        zerobus_stream_ingest_json_record(stream, sv("{not json"), &err),
        ZEROBUS_STATUS_FAILED_PRECONDITION);
    zerobus_error_free(err);

    /* Flush after close remains valid (nothing pending). */
    CHECK_EQ_INT(zerobus_stream_flush(stream, NULL), ZEROBUS_STATUS_OK);

    zerobus_stream_free(stream);
    zerobus_sdk_free(sdk);
}

/* Every stream-side *_free accepts NULL. */
static void test_stream_free_null_safe(void)
{
    zerobus_stream_builder_free(NULL);
    zerobus_stream_free(NULL);
    CHECK(1);
}

int main(void)
{
    test_stream_builder_validation();
    test_stream_builder_edges();
    test_stream_oauth_transactional();
    test_ingest_validation();
    test_flush_close_idempotent();
    test_stream_free_null_safe();
    TEST_MAIN_RETURN();
}
