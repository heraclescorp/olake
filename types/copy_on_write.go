package types

import (
	"fmt"
	"math"
	"time"

	"github.com/goccy/go-json"
)

type MergeStrategy string

const (
	MergeOnRead                MergeStrategy = "merge-on-read"
	CopyOnWrite                MergeStrategy = "copy-on-write"
	MaxCOWFlushIntervalSeconds               = int64(math.MaxInt64) / int64(time.Second)
)

// COWOptions bounds incoming batches, not source transactions. A transaction is
// always published whole; writers spill replayable typed segments when needed.
type COWOptions struct {
	FlushIntervalSeconds       int  `json:"flush_interval_seconds"`
	MaxBatchRows               int  `json:"max_batch_rows"`
	MaxBatchBytes              int  `json:"max_batch_bytes"`
	RewriteConcurrency         int  `json:"rewrite_concurrency"`
	MaterializeExistingDeletes bool `json:"materialize_existing_deletes"`
}

func DefaultCOWOptions() COWOptions {
	return COWOptions{FlushIntervalSeconds: 300, MaxBatchRows: 100000, MaxBatchBytes: 67108864, RewriteConcurrency: 2}
}

func (o *COWOptions) UnmarshalJSON(data []byte) error {
	type wire COWOptions
	value := wire(DefaultCOWOptions())
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	*o = COWOptions(value)
	return nil
}

func (s *ConfiguredStream) CopyOnWriteEnabled() bool {
	return s != nil && s.StreamMetadata.MergeStrategy == CopyOnWrite && !s.AppendModeEnabled()
}

func (s *ConfiguredStream) EffectiveCOWOptions() (COWOptions, error) {
	strategy := s.StreamMetadata.MergeStrategy
	if strategy != "" && strategy != MergeOnRead && strategy != CopyOnWrite {
		return COWOptions{}, fmt.Errorf("unsupported merge_strategy %q: choose merge-on-read or copy-on-write", strategy)
	}
	options := DefaultCOWOptions()
	if s.StreamMetadata.CowOptions != nil {
		options = *s.StreamMetadata.CowOptions
	}
	if options.FlushIntervalSeconds <= 0 || options.MaxBatchRows <= 0 || options.MaxBatchBytes <= 0 || options.RewriteConcurrency <= 0 {
		return COWOptions{}, fmt.Errorf("cow_options interval, row limit, byte limit and concurrency must be positive")
	}
	if int64(options.FlushIntervalSeconds) > MaxCOWFlushIntervalSeconds {
		return COWOptions{}, fmt.Errorf("cow_options.flush_interval_seconds must not exceed %d (maximum representable duration)", MaxCOWFlushIntervalSeconds)
	}
	return options, nil
}

// ValidateCopyOnWrite is read-only and must precede destination creation/reset.
// Source protocol and TOAST hydration support are validated by the source driver.
func (s *ConfiguredStream) ValidateCopyOnWrite(destination DestinationType) error {
	if _, err := s.EffectiveCOWOptions(); err != nil {
		return err
	}
	if s.StreamMetadata.MergeStrategy != CopyOnWrite || (s.StreamMetadata.AppendMode != nil && *s.StreamMetadata.AppendMode) {
		return nil
	}
	if s.AppendModeEnabled() {
		return fmt.Errorf("copy-on-write requires explicit Upsert (append_mode=false); append defaults cannot activate it")
	}
	if destination != Iceberg || !s.NormalizationEnabled() {
		return fmt.Errorf("copy-on-write requires normalized native Iceberg Upsert")
	}
	if s.Stream == nil || s.Stream.Schema == nil || s.Stream.SourceDefinedPrimaryKey == nil || s.Stream.SourceDefinedPrimaryKey.Len() == 0 {
		return fmt.Errorf("copy-on-write requires source-defined nonnull primary key columns")
	}
	seen := make(map[string]bool)
	for _, name := range s.Stream.SourceDefinedPrimaryKey.Array() {
		output := s.ResolveColumnName(name)
		if seen[output] || !s.IsSelectedColumn()(output) {
			return fmt.Errorf("copy-on-write primary key %q is unselected or collides after normalization", name)
		}
		seen[output] = true
		found, property := s.Stream.Schema.GetProperty(name)
		if !found {
			found, property = s.Stream.Schema.GetProperty(output)
		}
		if !found || property.Nullable() {
			return fmt.Errorf("copy-on-write primary key %q must be available and nonnull", name)
		}
		switch property.DataType().ValueType() {
		case Bool, Int32, Int64, String, Binary, Timestamp, TimestampMilli, TimestampMicro, TimestampNano:
		default:
			return fmt.Errorf("copy-on-write primary key %q has unsupported exact-filter type %s", name, property.DataType())
		}
	}
	return nil
}
