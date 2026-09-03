# Avro ingestion example (Beta)

Ingests pre-encoded Avro datums into an ephemeral stream using `AvroBytes`.

Avro support is behind the `avro` feature (enabled in this example's `Cargo.toml`)
and is **Beta**: ephemeral streams only, and server support is still pending.

```bash
cargo run --manifest-path Cargo.toml
```

Edit the constants at the top of `src/main.rs` (table, credentials, endpoints) and
supply real Avro-encoded records for your writer schema.
