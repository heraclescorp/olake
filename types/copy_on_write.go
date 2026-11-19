package types

import "fmt"

type MergeStrategy string

const (
	MergeOnRead MergeStrategy = "merge-on-read"
	CopyOnWrite MergeStrategy = "copy-on-write"
)

func (s *ConfiguredStream) CopyOnWriteEnabled() bool {
	return s != nil && s.StreamMetadata.MergeStrategy == CopyOnWrite && !s.AppendModeEnabled()
}

// ValidateCopyOnWrite is read-only and must precede destination creation/reset.
// Source protocol and TOAST hydration support are validated by the source driver.
func (s *ConfiguredStream) ValidateCopyOnWrite(destination DestinationType) error {
	strategy := s.StreamMetadata.MergeStrategy
	if strategy != "" && strategy != MergeOnRead && strategy != CopyOnWrite {
		return fmt.Errorf("unsupported merge_strategy %q: choose merge-on-read or copy-on-write", strategy)
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
	if s.Stream == nil || s.Stream.SourceDefinedPrimaryKey == nil || s.Stream.SourceDefinedPrimaryKey.Len() == 0 {
		return fmt.Errorf("copy-on-write requires a source-defined primary key")
	}
	return nil
}
