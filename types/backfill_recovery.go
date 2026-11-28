package types

import (
	"context"
	"encoding/json"
	"fmt"
)

const BackfillTableKey = "backfill_table"
const BackfillPlanKey = "backfill_plan"

type BackfillSourceBinding struct {
	SourceSHA256      string `json:"source_sha256"`
	DestinationSHA256 string `json:"destination_sha256"`
	CatalogSHA256     string `json:"catalog_sha256"`
	ServerIdentity    string `json:"server_identity"`
	SlotName          string `json:"slot_name"`
}

func (checkpoint *BackfillRecoveryCheckpoint) SourceBinding() BackfillSourceBinding {
	return BackfillSourceBinding{SourceSHA256: checkpoint.SourceSHA256, DestinationSHA256: checkpoint.DestinationSHA256, CatalogSHA256: checkpoint.CatalogSHA256, ServerIdentity: checkpoint.ServerIdentity, SlotName: checkpoint.SlotName}
}

type BackfillTableBinding struct {
	TableUUID   string `json:"table_uuid"`
	ShapeSHA256 string `json:"shape_sha256"`
}

type BackfillRecoveryCheckpoint struct {
	Version           int             `json:"version"`
	ExecutionID       string          `json:"execution_id"`
	LaunchID          string          `json:"launch_id"`
	SourceSHA256      string          `json:"source_sha256"`
	DestinationSHA256 string          `json:"destination_sha256"`
	CatalogSHA256     string          `json:"catalog_sha256"`
	StateSHA256       string          `json:"state_sha256"`
	State             json.RawMessage `json:"state"`
	ServerIdentity    string          `json:"server_identity"`
	SlotName          string          `json:"slot_name"`
}

func (s *State) GetBackfillTable(stream *ConfiguredStream) *BackfillTableBinding {
	value := s.streamValue(stream, BackfillTableKey)
	if binding, ok := value.(*BackfillTableBinding); ok {
		return binding
	}
	if value == nil {
		return nil
	}
	data, err := json.Marshal(value)
	if err != nil {
		return nil
	}
	var binding BackfillTableBinding
	if json.Unmarshal(data, &binding) != nil || binding.TableUUID == "" || binding.ShapeSHA256 == "" {
		return nil
	}
	return &binding
}

func (s *State) SetBackfillTable(stream *ConfiguredStream, binding *BackfillTableBinding) {
	s.storeStreamValue(stream, BackfillTableKey, binding)
}

func (s *State) GetBackfillPlan(stream *ConfiguredStream) *Set[Chunk] {
	return s.chunkSet(stream, BackfillPlanKey)
}

func (s *State) PersistBackfill() error {
	s.Lock()
	defer s.Unlock()
	s.LogState()
	return s.backfillPersistenceErr
}

func (s *State) ErrBackfillPersistence() error {
	if s == nil {
		return nil
	}
	s.RLock()
	defer s.RUnlock()
	return s.backfillPersistenceErr
}

func (s *State) SetBackfillPlan(stream *ConfiguredStream, plan *Set[Chunk]) error {
	if plan == nil || plan.Len() == 0 {
		return fmt.Errorf("backfill requires a nonempty immutable chunk plan")
	}
	s.Lock()
	defer s.Unlock()
	var entry *StreamState
	for _, candidate := range s.Streams {
		if candidate.Namespace == stream.Namespace() && candidate.Stream == stream.Name() {
			entry = candidate
			break
		}
	}
	if entry == nil {
		entry = s.initStreamState(stream)
		s.Streams = append(s.Streams, entry)
	}
	entry.State.Store(ResumePlanKey, plan)
	entry.State.Store(BackfillPlanKey, plan)
	entry.State.Store(ChunksKey, NewSet(plan.Array()...))
	entry.HoldsValue.Store(true)
	s.LogState()
	return s.backfillPersistenceErr
}

func (s *State) CompleteBackfill(streamID string) error {
	s.Lock()
	defer s.Unlock()
	if s.Global == nil {
		return fmt.Errorf("backfill completion requires the original GLOBAL checkpoint")
	}
	changed := s.Global.Streams == nil || !s.Global.Streams.Exists(streamID)
	if s.Global.Streams == nil {
		s.Global.Streams = NewSet[string]()
	}
	s.Global.Streams.Insert(streamID)
	for _, entry := range s.Streams {
		if entry.Namespace+"."+entry.Stream == streamID {
			if _, exists := entry.State.LoadAndDelete(ResumePlanKey); exists {
				changed = true
			}
			value, _ := entry.State.Load(ChunksKey)
			pending, ok := value.(*Set[Chunk])
			if !ok || pending == nil || pending.Len() != 0 {
				entry.State.Store(ChunksKey, NewSet[Chunk]())
				changed = true
			}
			entry.HoldsValue.Store(true)
		}
	}
	if changed {
		s.LogState()
	}
	return s.backfillPersistenceErr
}

type backfillCheckpointKey struct{}

func WithBackfillCheckpoint(ctx context.Context, checkpoint *BackfillRecoveryCheckpoint) context.Context {
	return context.WithValue(ctx, backfillCheckpointKey{}, checkpoint)
}

func BackfillCheckpointFromContext(ctx context.Context) (*BackfillRecoveryCheckpoint, bool) {
	checkpoint, ok := ctx.Value(backfillCheckpointKey{}).(*BackfillRecoveryCheckpoint)
	return checkpoint, ok && checkpoint != nil
}

type BackfillBindingJournalEntry struct {
	TableUUID   string          `json:"table_uuid"`
	ShapeSHA256 string          `json:"shape_sha256"`
	Plan        json.RawMessage `json:"plan"`
}

type BackfillBindingJournal struct {
	Read   func(string) (*BackfillBindingJournalEntry, error)
	Create func(string, *BackfillBindingJournalEntry) error
}

type BackfillRecoveryBinding struct {
	Version             int    `json:"version"`
	OriginalExecutionID string `json:"original_execution_id"`
	OriginalLaunchID    string `json:"original_launch_id"`
	SourceSHA256        string `json:"source_sha256"`
	DestinationSHA256   string `json:"destination_sha256"`
	CatalogSHA256       string `json:"catalog_sha256"`
	ServerIdentity      string `json:"server_identity"`
	SlotName            string `json:"slot_name"`
	LowerLSN            string `json:"lower_lsn"`
	Stream              string `json:"stream"`
	BackfillBindingJournalEntry
}

type backfillBindingJournalKey struct{}

func WithBackfillBindingJournal(ctx context.Context, journal *BackfillBindingJournal) context.Context {
	return context.WithValue(ctx, backfillBindingJournalKey{}, journal)
}

func BackfillBindingJournalFromContext(ctx context.Context) (*BackfillBindingJournal, bool) {
	journal, ok := ctx.Value(backfillBindingJournalKey{}).(*BackfillBindingJournal)
	return journal, ok && journal != nil
}
