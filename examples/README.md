# Zerobus Rust SDK Examples

This directory contains examples demonstrating how to use the Zerobus Rust SDK to ingest data into Databricks Delta tables.

## Available Examples

### 1. JSON Example (`basic_example_json/`)
**Recommended for getting started** - A simpler example that uses JSON for data serialization.

- No schema generation required
- Direct JSON string ingestion
- Easier to understand and modify
- Great for quick prototyping

### 2. Protocol Buffers Example (`basic_example_proto/`)
A more advanced example that uses Protocol Buffers for type-safe data serialization.

- Schema files already included (generation only needed for custom tables)
- Type-safe record creation
- Better for production use cases
- More efficient binary encoding

## Common Features

Both examples demonstrate:
- Creating a stream with OAuth authentication
- Ingesting a single record
- Waiting for acknowledgment
- Properly closing the stream
- Configuring credentials and endpoints

## Prerequisites

### 1. Create a Databricks Table

First, create a table in your Databricks workspace using the following SQL:

```sql
CREATE TABLE catalog.schema.orders (
  id INT,
  customer_name STRING,
  product_name STRING,
  quantity INT,
  price DOUBLE,
  status STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
);
```

Replace `catalog.schema.orders` with your actual catalog, schema, and table name.

### 2. Set Up OAuth Service Principal

You'll need a Databricks service principal with OAuth credentials:

1. In your Databricks workspace, go to **Settings** → **Identity and Access**
2. Create a service principal or use an existing one
3. Generate OAuth credentials (client ID and secret)
4. Grant the service principal the following permissions on your table:
   - `SELECT` - Read table schema
   - `MODIFY` - Write data to the table
   - `USE CATALOG` and `USE SCHEMA` - Access catalog and schema

### 3. Configure Credentials

Both examples require the same credentials. Edit the `src/main.rs` file in your chosen example directory and update these constants:

```rust
const DATABRICKS_WORKSPACE_URL: &str = "https://your-workspace.cloud.databricks.com";
const TABLE_NAME: &str = "catalog.schema.orders";
const DATABRICKS_CLIENT_ID: &str = "your-client-id";
const DATABRICKS_CLIENT_SECRET: &str = "your-client-secret";
const SERVER_ENDPOINT: &str = "https://workspace-id.zerobus.region.cloud.databricks.com";
```

**How to get these values:**
- **DATABRICKS_WORKSPACE_URL** - Your Databricks workspace URL (Unity Catalog endpoint)
- **TABLE_NAME** - Full table name in format `catalog.schema.table`
- **DATABRICKS_CLIENT_ID** - OAuth 2.0 client ID from your service principal
- **DATABRICKS_CLIENT_SECRET** - OAuth 2.0 client secret from your service principal
- **SERVER_ENDPOINT** - Zerobus ingestion endpoint (usually `https://<workspace-id>.zerobus.<region>databricks.com`)

---

## Running the JSON Example

The JSON example is simpler and doesn't require schema generation.

### Navigate to the JSON Example

```bash
cd examples/basic_example_json
```

### Run the Example

```bash
cargo run
```

**Expected output:**

```
Record acknowledged with offset Id: 0
Stream closed successfully
```

### Code Highlights

The JSON example uses string-based JSON records:

```rust
let json_record = format!(
    r#"{{
        "id": 1,
        "customer_name": "Alice Smith",
        "product_name": "Wireless Mouse",
        "quantity": 2,
        "price": 25.99,
        "status": "pending",
        "created_at": {},
        "updated_at": {}
    }}"#,
    now, now
);

let ack_future = stream.ingest_record(json_record).await.unwrap();
```

Key differences from the Protocol Buffers example:
- Set `record_type: RecordType::Json` in `StreamConfigurationOptions`
- No descriptor file needed
- Pass JSON strings directly to `ingest_record()`

---

## Running the Protocol Buffers Example

The Protocol Buffers example provides type safety and better performance. **No schema generation needed** - the example already includes all generated files in the `output/` folder.

### Navigate to the Proto Example

```bash
cd examples/basic_example_proto
```

### Run the Example

Simply run the example as-is:

```bash
cargo run
```

**Expected output:**

```
Record acknowledged with offset Id: 0
Stream closed successfully
```

Schema generation is only needed if you want to customize it for your own table (see "Adapting for Your Custom Table" below).

### Code Highlights

The Protocol Buffers example uses strongly-typed structs:

```rust
let ack_future = stream
    .ingest_record(
        TableOrders {
            id: Some(1),
            customer_name: Some("Alice Smith".to_string()),
            product_name: Some("Wireless Mouse".to_string()),
            quantity: Some(2),
            price: Some(25.99),
            status: Some("pending".to_string()),
            created_at: Some(chrono::Utc::now().timestamp()),
            updated_at: Some(chrono::Utc::now().timestamp()),
        }
        .encode_to_vec(),
    )
    .await
    .unwrap();
```

Key features:
- Type-safe record creation with compile-time checks
- Efficient binary encoding via Protocol Buffers
- Requires descriptor file and generated Rust structs

---

## Adapting for Your Custom Table

### For JSON Example

Simply modify the JSON string in `src/main.rs` to match your table's schema:

```rust
let json_record = format!(
    r#"{{
        "your_field_1": "value1",
        "your_field_2": 123,
        "your_field_3": true
    }}"#
);
```

No schema generation needed!

### For Protocol Buffers Example

To use your own custom table, you'll need to generate schema files for it and update `src/main.rs`.

**Step 1: Generate Schema Files for Your Table**

Run this from the repository root directory:

```bash
cd tools/generate_files

cargo run -- \
  --uc-endpoint "https://<your-workspace-id>.zerobus.<region>.cloud.databricks.com" \
  --uc-token "<your_pat_token>" \
  --table "<catalog.schema.your_table>" \
  --output-dir "../../examples/basic_example_proto/output"
```

This generates:
- `output/<your_table>.proto` - Protocol Buffer schema
- `output/<your_table>.rs` - Rust structs
- `output/<your_table>.descriptor` - Binary descriptor file

**Step 2: Update `src/main.rs`**

**A. Update the `mod` and `use` statements:**
Change `orders` to match the name of your generated Rust file (e.g., `your_table`).

*Before:*
```rust
pub mod orders {
    include!("../output/orders.rs");
}
use crate::orders::TableOrders;
```

*After (for a table named `inventory`):*
```rust
pub mod inventory {
    include!("../output/inventory.rs");
}
use crate::inventory::TableInventory;
```

**B. Update the `load_descriptor_proto` call:**
Change the filenames and message name to match your generated files.

*Before:*
```rust
let descriptor_proto = load_descriptor_proto(
    "output/orders.descriptor",
    "orders.proto",
    "table_Orders"
);
```

*After (for a table named `inventory`):*
```rust
let descriptor_proto = load_descriptor_proto(
    "output/inventory.descriptor",
    "inventory.proto",
    "table_Inventory"
);
```

**C. Update the record creation:**
Modify the code to create an instance of your new table struct with your own data.

*Before:*
```rust
let ack_future = stream.ingest_record(
    TableOrders {
        id: Some(1),
        customer_name: Some("Alice Smith".to_string()),
        // ... other fields
    }.encode_to_vec()
).await.unwrap();
```

*After (for a table named `inventory`):*
```rust
let ack_future = stream.ingest_record(
    TableInventory {
        item_id: Some(123),
        sku: Some("SKU-XYZ".to_string()),
        // ... other fields
    }.encode_to_vec()
).await.unwrap();
```

---

## Common Code Patterns

Both examples follow the same general flow:

### 1. Configure Table Properties

**JSON Example:**
```rust
let table_properties = TableProperties {
    table_name: TABLE_NAME.to_string(),
    descriptor_proto: None, // Not needed for JSON
};
```

**Protocol Buffers Example:**
```rust
let descriptor_proto = load_descriptor_proto(
    "output/orders.descriptor",
    "orders.proto",
    "table_Orders"
);
let table_properties = TableProperties {
    table_name: TABLE_NAME.to_string(),
    descriptor_proto,
};
```

### 2. Configure Stream Options

**JSON Example:**
```rust
let stream_configuration_options = StreamConfigurationOptions {
    max_inflight_records: 100,
    record_type: RecordType::Json,  // Important for JSON!
    ..Default::default()
};
```

**Protocol Buffers Example:**
```rust
let stream_configuration_options = StreamConfigurationOptions {
    max_inflight_records: 100,
    ..Default::default()  // RecordType::Proto is the default
};
```

### 3. Initialize SDK

```rust
let sdk_handle = ZerobusSdk::new(
    SERVER_ENDPOINT.to_string(),
    DATABRICKS_WORKSPACE_URL.to_string(),
)?;
```

Creates the SDK instance with Zerobus and Unity Catalog endpoints.

### 4. Create Stream

```rust
let mut stream = sdk_handle
    .create_stream(
        table_properties,
        DATABRICKS_CLIENT_ID.to_string(),
        DATABRICKS_CLIENT_SECRET.to_string(),
        Some(stream_configuration_options),
    )
    .await
    .expect("Failed to create a stream.");
```

Opens a bidirectional gRPC stream with OAuth authentication.

### 5. Ingest Record

**JSON Example:**
```rust
let json_record = format!(r#"{{"id": 1, "name": "Alice"}}"#);
let ack_future = stream.ingest_record(json_record).await.unwrap();
```

**Protocol Buffers Example:**
```rust
let ack_future = stream
    .ingest_record(
        TableOrders {
            id: Some(1),
            customer_name: Some("Alice Smith".to_string()),
            // ... other fields
        }
        .encode_to_vec(),
    )
    .await
    .unwrap();
```

Returns a future that resolves when the server acknowledges the record.

### 6. Wait for Acknowledgment

```rust
let _ack = ack_future.await.unwrap();
```

Blocks until the server confirms the record was written.

### 7. Close Stream

```rust
stream.close().await?;
```

Flushes pending records and closes the stream gracefully.

## Advanced Usage

### Ingest Multiple Records

**JSON Example:**
```rust
for i in 0..100 {
    let json_record = format!(
        r#"{{"id": {}, "customer_name": "Customer {}"}}"#,
        i, i
    );
    let _ack_future = stream.ingest_record(json_record).await?;
}

// Flush all pending records
stream.flush().await?;
```

**Protocol Buffers Example:**
```rust
for i in 0..100 {
    let _ack_future = stream
        .ingest_record(
            TableOrders {
                id: Some(i),
                customer_name: Some(format!("Customer {}", i)),
                // ... other fields
            }
            .encode_to_vec(),
        )
        .await?;
}

// Flush all pending records
stream.flush().await?;
```

## Choosing Between JSON and Protocol Buffers

| Feature | JSON Example | Protocol Buffers Example |
|---------|-------------|-------------------------|
| **Setup Complexity** | Simple - no schema files needed | Works out of the box (schema files included) |
| **Type Safety** | Runtime validation only | Compile-time type checking |
| **Performance** | Text-based encoding | Efficient binary encoding |
| **Flexibility** | Easy to modify records on-the-fly | Requires regenerating schema for custom tables |
| **Best For** | Prototyping, simple use cases | Production, high-throughput scenarios |
| **Learning Curve** | Low | Moderate |

**Recommendation:** Start with the JSON example for quick prototyping, then migrate to Protocol Buffers for production deployments where type safety and performance matter.

**Note:** Both examples work immediately out of the box. Schema generation is only required when customizing the Protocol Buffers example for your own table.

## Troubleshooting

### Error: "Failed to create a stream"

**Possible causes:**
- Invalid credentials (client ID or secret)
- Service principal lacks permissions on the table
- Incorrect workspace URL or endpoint
- Table doesn't exist

**Solution:** Verify your credentials and table permissions.

### Error: "Failed to read proto descriptor file" (Protocol Buffers only)

**Possible causes:**
- Schema files not generated
- Wrong file paths in `load_descriptor_proto()`

**Solution:** Run the schema generation tool and verify the `output/` directory contains the generated files.

### Error: "Invalid token"

**Possible causes:**
- OAuth credentials expired or invalid
- Incorrect Unity Catalog endpoint

**Solution:** Regenerate your service principal credentials and verify the endpoint URL.

### Error: JSON parsing errors (JSON example only)

**Possible causes:**
- JSON string doesn't match table schema
- Invalid JSON syntax
- Type mismatches (e.g., passing string instead of number)

**Solution:** Verify your JSON structure matches the Databricks table schema exactly.

## Next Steps

- Try ingesting larger batches of records
- Experiment with different `StreamConfigurationOptions`
- Add error handling and retry logic
- Implement monitoring and metrics
- Use the SDK in a production application

## Additional Resources

- [Main SDK Documentation](../../README.md)
- [Schema Generation Tool](../../tools/generate_files/README.md)
- [Databricks Unity Catalog Documentation](https://docs.databricks.com/unity-catalog/index.html)
