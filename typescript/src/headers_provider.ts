/**
 * Custom headers provider accepted by `createStream()`.
 *
 * The native adapter invokes `getHeadersCallback` synchronously once during
 * stream creation and stores the returned tuples. Returning a Promise, or
 * passing a class with async `getHeaders()`, is not supported and can terminate
 * the process. Token refresh is not currently wired through this callback.
 */
export interface HeadersProvider {
    /**
     * Returns headers as an array of [name, value] tuples.
     *
     * Required headers:
     * - ["authorization", "Bearer <token>"]
     * - ["x-databricks-zerobus-table-name", "<table_name>"]
     */
    getHeadersCallback: () => Array<[string, string]>;
}

/**
 * OAuth 2.0 Client Credentials headers provider.
 *
 * Do not instantiate this class or pass it to `createStream()`.
 *
 * OAuth authentication is handled automatically by the Rust SDK when you call
 * `createStream()` with clientId and clientSecret and omit the headers provider.
 *
 * How to use OAuth authentication:
 * ```typescript
 * const stream = await sdk.createStream(
 *     tableProperties,
 *     clientId,
 *     clientSecret,
 *     options
 * );
 * ```
 *
 * How to use custom authentication (PAT or a static token):
 * ```typescript
 * const stream = await sdk.createStream(
 *     tableProperties,
 *     '',
 *     '',
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
export class OAuthHeadersProvider {
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
            'To use OAuth: call createStream(tableProperties, clientId, clientSecret, options) without a headers provider. ' +
            'To use custom authentication: pass { getHeadersCallback: () => [...] } as the headers provider.'
        );
    }
}
