//go:build avro

package zerobus

// AvroRecord is a map-shaped Avro record the stream encodes against the writer
// schema declared via WithAvro. Field names must match that schema. Requires
// the `avro` build tag.
type AvroRecord map[string]any
