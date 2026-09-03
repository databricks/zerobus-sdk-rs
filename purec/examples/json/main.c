/*
 * Minimal end-to-end JSON ingestion example: build an SDK, open an
 * OAuth-authenticated ephemeral JSON stream, queue a few records without
 * waiting after each one, flush once, then close and free everything.
 *
 * NOTE: the SDK is under early development. The operations validate inputs and
 * return the documented statuses but perform no network I/O yet, so this shows
 * the API shape and lifecycle rather than delivering records.
 *
 * The table schema must match the two example JSON fields (id, message), or the
 * records must be replaced with ones matching the selected table.
 */
#include <stdio.h>
#include <string.h>

#include <zerobus/zerobus.h>

static zerobus_string_view_t string_view(const char *value)
{
    return (zerobus_string_view_t){value, strlen(value)};
}

static void print_error(const zerobus_error_t *error)
{
    if (error == NULL) {
        fputs("zerobus: operation failed without details\n", stderr);
        return;
    }
    zerobus_string_view_t message = zerobus_error_message(error);
    fprintf(stderr, "zerobus: %.*s\n", (int)message.len, message.data);
}

int main(int argc, char **argv)
{
    if (argc != 6) {
        fprintf(stderr,
                "usage: %s <zerobus-endpoint> <uc-endpoint> "
                "<table> <client-id> <client-secret>\n",
                argv[0]);
        return 2;
    }

    const char *records[] = {
        "{\"id\":1,\"message\":\"hello\"}",
        "{\"id\":2,\"message\":\"from C\"}",
    };

    zerobus_error_t *error = NULL;
    zerobus_sdk_builder_t *sdk_builder = NULL;
    zerobus_stream_builder_t *stream_builder = NULL;
    zerobus_sdk_t *sdk = NULL;
    zerobus_stream_t *stream = NULL;
    zerobus_status_t status;
    int exit_code = 1;

    status = zerobus_sdk_builder_new(&sdk_builder, &error);
    if (status != ZEROBUS_STATUS_OK) {
        goto fail;
    }

    status = zerobus_sdk_builder_set_endpoint(sdk_builder, string_view(argv[1]),
                                              &error);
    if (status != ZEROBUS_STATUS_OK) {
        goto fail;
    }

    status = zerobus_sdk_builder_set_unity_catalog_endpoint(
        sdk_builder, string_view(argv[2]), &error);
    if (status != ZEROBUS_STATUS_OK) {
        goto fail;
    }

    status = zerobus_sdk_builder_build(sdk_builder, &sdk, &error);
    if (status != ZEROBUS_STATUS_OK) {
        goto fail;
    }

    zerobus_sdk_builder_free(sdk_builder);
    sdk_builder = NULL;

    status = zerobus_stream_builder_new(sdk, &stream_builder, &error);
    if (status != ZEROBUS_STATUS_OK) {
        goto fail;
    }

    status = zerobus_stream_builder_set_table(stream_builder,
                                              string_view(argv[3]), &error);
    if (status != ZEROBUS_STATUS_OK) {
        goto fail;
    }

    status = zerobus_stream_builder_set_oauth(
        stream_builder, string_view(argv[4]), string_view(argv[5]), &error);
    if (status != ZEROBUS_STATUS_OK) {
        goto fail;
    }

    status = zerobus_stream_builder_build(stream_builder, &stream, &error);
    if (status != ZEROBUS_STATUS_OK) {
        goto fail;
    }

    zerobus_stream_builder_free(stream_builder);
    stream_builder = NULL;

    /* Queue all records. Do NOT wait for each record's acknowledgment. */
    for (size_t i = 0; i < sizeof(records) / sizeof(records[0]); ++i) {
        status = zerobus_stream_ingest_json_record(
            stream, string_view(records[i]), &error);
        if (status != ZEROBUS_STATUS_OK) {
            goto fail;
        }
    }

    /* One durability barrier for everything admitted above. */
    status = zerobus_stream_flush(stream, &error);
    if (status != ZEROBUS_STATUS_OK) {
        goto fail;
    }

    status = zerobus_stream_close(stream, &error);
    if (status != ZEROBUS_STATUS_OK) {
        goto fail;
    }

    printf("ingested %zu records\n", sizeof(records) / sizeof(records[0]));
    exit_code = 0;

fail:
    if (error != NULL) {
        print_error(error);
        zerobus_error_free(error);
    }
    zerobus_stream_builder_free(stream_builder);
    zerobus_sdk_builder_free(sdk_builder);
    zerobus_stream_free(stream);
    zerobus_sdk_free(sdk);
    return exit_code;
}
