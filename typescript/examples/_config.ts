/**
 * Shared configuration for examples.
 *
 * Reads environment variables and returns ready-to-use SdkOptions + Auth + table
 * for the example to consume. Supports two modes:
 *
 * 1. **Production / Databricks workspace** (default):
 *      ZEROBUS_ENDPOINT=https://<workspace>.zerobus.<region>.cloud.databricks.com
 *      DATABRICKS_WORKSPACE_URL=https://<workspace>.cloud.databricks.com
 *      DATABRICKS_CLIENT_ID=<oauth client id>
 *      DATABRICKS_CLIENT_SECRET=<oauth client secret>
 *      ZEROBUS_TABLE_NAME=catalog.schema.table
 *
 * 2. **Local test server** (no auth, no TLS):
 *      ZEROBUS_ENDPOINT=http://[::1]:50051
 *      ZEROBUS_TLS=none
 *      ZEROBUS_NO_AUTH=1
 *      ZEROBUS_TABLE_NAME=test_data/test_table
 */

import type { SdkOptions, Auth } from '../dist/index.js';

export interface ExampleConfig {
    sdkOptions: SdkOptions;
    auth: Auth;
    tableName: string;
}

export function loadConfig(): ExampleConfig {
    const endpoint = process.env.ZEROBUS_ENDPOINT ?? 'http://[::1]:50051';
    const unityCatalogUrl = process.env.DATABRICKS_WORKSPACE_URL;
    const tls: 'secure' | 'none' = process.env.ZEROBUS_TLS === 'none' ? 'none' : 'secure';
    const tableName = process.env.ZEROBUS_TABLE_NAME ?? 'test_data/test_table';

    const sdkOptions: SdkOptions = {
        endpoint,
        unityCatalogUrl,
        tls,
        applicationName: 'zerobus-sdk-ts-example/2.0.0',
    };

    let auth: Auth;
    if (process.env.ZEROBUS_NO_AUTH === '1' || tls === 'none') {
        auth = { type: 'noAuth' };
    } else {
        const clientId = process.env.DATABRICKS_CLIENT_ID;
        const clientSecret = process.env.DATABRICKS_CLIENT_SECRET;
        if (!clientId || !clientSecret) {
            console.error(
                'Set DATABRICKS_CLIENT_ID and DATABRICKS_CLIENT_SECRET, or set ZEROBUS_NO_AUTH=1 for a local server.',
            );
            process.exit(1);
        }
        auth = { type: 'oauth', clientId, clientSecret };
    }

    console.log(`[config] endpoint=${endpoint} tls=${tls} auth=${auth.type} table=${tableName}`);
    return { sdkOptions, auth, tableName };
}
