package com.databricks.zerobus;

import java.util.Map;

/**
 * Supplies custom request headers for a Zerobus stream.
 *
 * <p>Implement this interface to use authentication other than the built-in OAuth client
 * credentials flow, such as a personal access token or a token from a custom identity provider.
 * Implementations must include the {@code authorization} and {@code
 * x-databricks-zerobus-table-name} headers.
 *
 * <pre>{@code
 * HeadersProvider provider = () -> {
 *     Map<String, String> headers = new HashMap<>();
 *     headers.put("authorization", "Bearer " + fetchToken());
 *     headers.put("x-databricks-zerobus-table-name", "catalog.schema.table");
 *     return headers;
 * };
 *
 * ZerobusJsonStream stream = sdk.streamBuilder()
 *     .table("catalog.schema.table")
 *     .headersProvider(provider)
 *     .json()
 *     .build()
 *     .join();
 * }</pre>
 *
 * <p>Implementations must be thread-safe because methods may be called from internal SDK threads,
 * including during stream recovery.
 */
@FunctionalInterface
public interface HeadersProvider {

  /**
   * Returns the headers to attach to requests for the stream.
   *
   * <p>Header names must be valid gRPC metadata keys and should come from a small, fixed set.
   * Dynamically generated header names are not supported because names are retained for the
   * lifetime of the process.
   *
   * <p>Exceptions are treated as retryable during automatic stream recovery. Throw {@link
   * NonRetriableException} for permanent failures that should stop recovery immediately.
   *
   * @return a non-null map of header names to values
   * @throws Exception if the headers cannot be produced
   */
  Map<String, String> getHeaders() throws Exception;

  /**
   * Invalidates cached authentication state.
   *
   * <p>The SDK calls this after an authentication rejection so the next {@link #getHeaders()} call
   * can refresh credentials. Providers without cached state can use the default no-op.
   */
  default void invalidate() {}
}
