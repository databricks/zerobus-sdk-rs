module zerobus-examples/json-batch

go 1.25.3

require github.com/databricks/zerobus-sdk/go v0.1.0

require google.golang.org/protobuf v1.36.11 // indirect

// Use local zerobus module
replace github.com/databricks/zerobus-sdk/go => ../../..
