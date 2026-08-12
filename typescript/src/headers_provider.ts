/**
 * Interface for providing custom headers to Zerobus streams.
 *
 * Implement this interface to use custom authentication beyond OAuth,
 * such as Personal Access Tokens (PAT) or custom auth tokens.
 */
export interface HeadersProvider {
    /**
     * Returns headers as array of [name, value] tuples.
     *
     * Required headers:
     * - ["authorization", "Bearer <token>"]
     * - ["x-databricks-zerobus-table-name", "<table_name>"]
     *
     * @returns Promise resolving to array of header name-value pairs
     */
    getHeaders(): Promise<Array<[string, string]>>;
}

/**
 * OAuth 2.0 Client Credentials headers provider.
 *
 * **IMPORTANT: DO NOT instantiate this class directly.**
 *
 * OAuth authentication is handled automatically by the Rust SDK when you call
 * `createStream()` with clientId and clientSecret parameters (without providing
 * a headers_provider).
 *
 * This class exists for:
 * 1. Documentation purposes - showing the HeadersProvider pattern
 * 2. API consistency with other Zerobus SDKs (Python, Java, Rust)
 *
 * **How to use OAuth authentication:**
 * ```typescript
 * // OAuth is the default - just pass clientId and clientSecret
 * const stream = await sdk.createStream(
 *     tableProperties,
 *     clientId,      // OAuth client ID
 *     clientSecret,  // OAuth client secret
 *     options
 *     // No headers_provider parameter = OAuth authentication
 * );
 * ```
 *
 * The native `createStream` adapter currently requires its callback to return
 * header tuples synchronously. Pass a callback object directly:
 * ```typescript
 * const stream = await sdk.createStream(
 *     tableProperties,
 *     '', // ignored
 *     '', // ignored
 *     options,
 *     {
 *         getHeadersCallback: () => [
 *             ["authorization", `Bearer ${myToken}`],
 *             ["x-databricks-zerobus-table-name", tableName]
 *         ]
 *     }
 * );
 * ```
 */
export class OAuthHeadersProvider implements HeadersProvider {
    constructor(
        private clientId: string,
        private clientSecret: string,
        private tableName: string,
        private workspaceUrl: string
    ) {}

    async getHeaders(): Promise<Array<[string, string]>> {
        throw new Error(
            'OAuthHeadersProvider should not be instantiated directly. ' +
            'OAuth authentication is handled internally by the Rust SDK. ' +
            'To use OAuth: call createStream(tableProperties, clientId, clientSecret, options) without the headers_provider parameter. ' +
            'To use custom authentication: implement the HeadersProvider interface.'
        );
    }
}
