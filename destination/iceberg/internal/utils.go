package internal

// PartitionInfo represents an Iceberg partition column with its transform, preserving order.
// Field is the original source column name (used for record.Data lookups before pre-shaping).
// SchemaField is the resolved destination column name used by the native writer
// and record.Data after pre-shaping.
type PartitionInfo struct {
	Field       string // original case — matches source record.Data keys
	SchemaField string // resolved — matches Iceberg schema field names
	Transform   string
}
