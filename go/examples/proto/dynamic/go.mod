module zerobus-examples/proto-dynamic

go 1.23

require (
	github.com/databricks/zerobus-sdk/go v0.1.0
	google.golang.org/protobuf v1.36.11
)

// Use local zerobus module
replace github.com/databricks/zerobus-sdk/go => ../../..
