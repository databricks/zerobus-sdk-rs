module zerobus-examples/proto-single

go 1.25.3

require (
	github.com/databricks/zerobus-sdk/go v0.1.0
	google.golang.org/protobuf v1.36.10
	zerobus-examples v0.0.0
)

// Use local zerobus module
replace github.com/databricks/zerobus-sdk/go => ../../..

// Use local proto module
replace zerobus-examples => ..
