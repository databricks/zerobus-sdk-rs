# Token-cache staging harness

This opt-in harness verifies both token-cache regressions against a real
staging Zerobus endpoint without ingesting records. It opens and closes three
JSON streams against the same table and SDK instance.

A loopback-only OAuth proxy forwards the first token request to the configured
staging workspace over HTTPS, rewrites its `expires_in` value as a JSON string,
then returns HTTP 429 for the proactive refresh request. The second and third
streams are opened concurrently: both must use the cached, unexpired token, and
the caller waiting behind the failed refresh must not make another token request.
The proxy never logs credentials or tokens.

Use a staging service principal and a disposable staging table:

```bash
export DATABRICKS_CLIENT_ID='<staging-service-principal-id>'
export DATABRICKS_CLIENT_SECRET='<staging-service-principal-secret>'
export DATABRICKS_WORKSPACE_URL='https://<staging-workspace>'
export ZEROBUS_ENDPOINT='https://<staging-zerobus-endpoint>'
export DATABRICKS_TABLE_NAME='<catalog>.<schema>.<table>'

cargo run -p zerobus-token-cache-staging-harness
```

Success means all three streams opened and the proxy observed exactly two token
requests: the initial mint and one injected failed refresh. The concurrent
caller must not repeat the failed refresh.
