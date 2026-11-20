package iceberg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	apacheiceberg "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
)

const nativeBackfillOwnerProperty = "olake.backfill-owner"
const nativeBackfillBindingProperty = "olake.backfill-binding"

type nativeBackfillOwner struct {
	Version              int    `json:"version"`
	ExecutionID          string `json:"execution_id"`
	LaunchID             string `json:"launch_id"`
	SourceSHA256         string `json:"source_sha256"`
	DestinationSHA256    string `json:"destination_sha256"`
	CatalogSHA256        string `json:"catalog_sha256"`
	SourceIdentity       string `json:"source_identity"`
	SlotName             string `json:"slot_name"`
	LowerLSN             string `json:"lower_lsn"`
	Stream               string `json:"stream"`
	RequestedShapeSHA256 string `json:"requested_shape_sha256"`
	Plan                 string `json:"plan"`
}

type nativeBackfillBinding struct {
	Owner       nativeBackfillOwner `json:"owner"`
	TableUUID   string              `json:"table_uuid"`
	ShapeSHA256 string              `json:"shape_sha256"`
}

func backfillOwner(checkpoint *types.BackfillRecoveryCheckpoint, stream types.StreamInterface, plan string) (*nativeBackfillOwner, error) {
	var state struct {
		Global struct {
			State struct {
				LSN string `json:"lsn"`
			} `json:"state"`
		} `json:"global"`
	}
	if checkpoint == nil || json.Unmarshal(checkpoint.State, &state) != nil || checkpoint.Version != 1 || checkpoint.ExecutionID == "" || checkpoint.LaunchID == "" || checkpoint.SourceSHA256 == "" || checkpoint.DestinationSHA256 == "" || checkpoint.CatalogSHA256 == "" || checkpoint.ServerIdentity == "" || checkpoint.SlotName == "" || plan == "" {
		return nil, errors.New("original backfill table ownership requires the durable source checkpoint and exact plan")
	}
	if _, err := ordinaryRecoveryLSN(state.Global.State.LSN); err != nil {
		return nil, err
	}
	return &nativeBackfillOwner{Version: 1, ExecutionID: checkpoint.ExecutionID, LaunchID: checkpoint.LaunchID, SourceSHA256: checkpoint.SourceSHA256, DestinationSHA256: checkpoint.DestinationSHA256, CatalogSHA256: checkpoint.CatalogSHA256, SourceIdentity: checkpoint.ServerIdentity, SlotName: checkpoint.SlotName, LowerLSN: state.Global.State.LSN, Stream: stream.ID(), Plan: plan}, nil
}

func recoveryBackfillOwner(options *types.OrdinaryRecoveryOptions, stream types.StreamInterface, lower, plan string) *nativeBackfillOwner {
	origin := options.Origin
	return &nativeBackfillOwner{Version: 1, ExecutionID: origin.OriginalExecutionID, LaunchID: origin.OriginalLaunchID, SourceSHA256: origin.SourceSHA256, DestinationSHA256: origin.DestinationSHA256, CatalogSHA256: origin.CatalogSHA256, SourceIdentity: options.SourceIdentity, SlotName: options.BackfillCheckpoint.SlotName, LowerLSN: lower, Stream: stream.ID(), Plan: plan}
}

func decodeBackfillProperty(raw string, value any) error {
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return errors.New("backfill ownership property contains trailing data")
	}
	return nil
}

func requestedBackfillShape(config *Config, stream types.StreamInterface) (string, error) {
	columns, err := nativeStreamColumns(stream)
	if err != nil {
		return "", err
	}
	schema, err := nativeSchema(columns, !config.NoIdentifierFields)
	if err != nil {
		return "", err
	}
	spec, err := nativePartitionSpec(stream, schema)
	if err != nil {
		return "", err
	}
	order, err := stream.Self().ResolveSortOrder(types.Iceberg)
	if err != nil {
		return "", err
	}
	sortOrder, err := nativeSortOrder(order, schema)
	if err != nil {
		return "", err
	}
	version := "2"
	if !stream.Self().CopyOnWriteEnabled() && stream.GetUpdateType() == types.UpdateTypeDeletionVector {
		version = "3"
	}
	metadata, err := table.NewMetadata(schema, &spec, sortOrder, "", apacheiceberg.Properties{table.PropertyFormatVersion: version})
	if err != nil {
		return "", err
	}
	return ordinaryRecoveryShapeAtLocation(metadata, "")
}

func validateBackfillOwner(owner nativeBackfillOwner, expected nativeBackfillOwner) error {
	if owner != expected {
		return errors.New("backfill ownership differs from retired execution, source, exact plan or requested shape")
	}
	plan, err := decodeBackfillPlan(owner.Plan)
	if err != nil || plan == nil || plan.Len() == 0 {
		return errors.New("backfill ownership has no valid full immutable plan")
	}
	return nil
}

func pristineBackfillTable(tbl *table.Table) error {
	if tbl.CurrentSnapshot() != nil || len(tbl.Metadata().Snapshots()) != 0 || tbl.Properties()[nativeRecoveryProperty] != "" || tbl.Properties()[ordinaryRecoveryProperty] != "" {
		return errors.New("unbound backfill ownership can adopt only a pristine table without snapshots or recovery state")
	}
	for range tbl.Metadata().SnapshotLogs() {
		return errors.New("unbound backfill ownership table has snapshot history")
	}
	for range tbl.Metadata().PreviousFiles() {
		return errors.New("unbound backfill ownership table has metadata history")
	}
	return nil
}

func validateBackfillBinding(tbl *table.Table, binding nativeBackfillBinding, expected nativeBackfillOwner) error {
	if err := validateBackfillOwner(binding.Owner, expected); err != nil {
		return err
	}
	shape, err := ordinaryRecoveryShape(tbl.Metadata())
	if err != nil {
		return err
	}
	if binding.TableUUID != tbl.Metadata().TableUUID().String() || binding.ShapeSHA256 != shape {
		return errors.New("immutable backfill binding table UUID or shape changed")
	}
	return nil
}

func savedBackfillAuthority(checkpoint *types.BackfillRecoveryCheckpoint, stream types.StreamInterface, plan string) (*types.BackfillTableBinding, bool, error) {
	state := types.State{RWMutex: &sync.RWMutex{}}
	if err := json.Unmarshal(checkpoint.State, &state); err != nil {
		return nil, false, err
	}
	binding := state.GetBackfillTable(stream.Self())
	retained := state.GetBackfillPlan(stream.Self())
	if binding == nil && retained == nil {
		return nil, false, nil
	}
	if state.Type != types.GlobalType || state.BackfillSource == nil || *state.BackfillSource != checkpoint.SourceBinding() {
		return nil, false, errors.New("saved backfill progress is not bound to the current configuration and live source")
	}
	if retained == nil || retained.Len() == 0 {
		return nil, false, errors.New("saved backfill progress has no exact retained plan")
	}
	encoded, err := encodeBackfillPlan(stream, retained)
	if err != nil {
		return nil, false, err
	}
	if encoded != plan {
		return nil, false, errors.New("scheduled backfill retry changed the saved immutable plan")
	}
	return binding, true, nil
}

func validateBackfillOwnerWithAuthority(owner, expected nativeBackfillOwner, authority bool) error {
	if authority {
		if strings.TrimSpace(owner.ExecutionID) == "" || strings.TrimSpace(owner.LaunchID) == "" {
			return errors.New("immutable backfill ownership has no original execution identity")
		}
		owner.ExecutionID, owner.LaunchID = expected.ExecutionID, expected.LaunchID
	}
	return validateBackfillOwner(owner, expected)
}

func validateBackfillBindingWithAuthority(tbl *table.Table, binding nativeBackfillBinding, expected nativeBackfillOwner, authority bool) error {
	if err := validateBackfillOwnerWithAuthority(binding.Owner, expected, authority); err != nil {
		return err
	}
	binding.Owner = expected
	return validateBackfillBinding(tbl, binding, expected)
}

func (i *Iceberg) bindBackfillTable(ctx context.Context, stream types.StreamInterface, tbl *table.Table, owner nativeBackfillOwner, candidate *types.BackfillTableBinding, savedAuthority bool) (*table.Table, string, error) {
	if err := utils.CheckExecutionFence(ctx); err != nil {
		return nil, "", err
	}
	external, err := readBackfillBindingJournal(ctx, stream.ID())
	if err != nil {
		return nil, "", err
	}
	if err := checkBackfillBindingJournal(external, tbl, owner.Plan); err != nil {
		return nil, "", err
	}
	finish := func(current *table.Table, raw string) (*table.Table, string, error) {
		if err := persistBackfillBindingJournal(ctx, stream.ID(), current, owner.Plan); err != nil {
			return nil, "", err
		}
		return current, raw, nil
	}
	validate := func(current *table.Table) (string, error) {
		actual, err := backfillTableBinding(current, stream)
		if err != nil {
			return "", err
		}
		if candidate != nil && *actual != *candidate {
			return "", errors.New("backfill table differs from the immutable candidate binding")
		}
		if err := checkBackfillBindingJournal(external, current, owner.Plan); err != nil {
			return "", err
		}
		if raw := current.Properties()[nativeBackfillBindingProperty]; raw != "" {
			var stored nativeBackfillBinding
			if err := decodeBackfillProperty(raw, &stored); err != nil {
				return "", err
			}
			return raw, validateBackfillBindingWithAuthority(current, stored, owner, savedAuthority || candidate != nil || external != nil)
		}
		if candidate == nil && external == nil {
			var intent nativeBackfillOwner
			if err := decodeBackfillProperty(current.Properties()[nativeBackfillOwnerProperty], &intent); err != nil {
				return "", err
			}
			if err := validateBackfillOwnerWithAuthority(intent, owner, savedAuthority); err != nil {
				return "", err
			}
			shape, err := ordinaryRecoveryShapeAtLocation(current.Metadata(), "")
			if err != nil || shape != owner.RequestedShapeSHA256 {
				return "", errors.New("unbound backfill table differs from the frozen requested shape")
			}
			if err := pristineBackfillTable(current); err != nil {
				return "", err
			}
		}
		return "", nil
	}
	if raw, err := validate(tbl); err != nil {
		return nil, "", err
	} else if raw != "" {
		return finish(tbl, raw)
	}
	fresh, err := i.catalog.LoadTable(ctx, tbl.Identifier())
	if err != nil {
		return nil, "", err
	}
	if raw, err := validate(fresh); err != nil {
		return nil, "", err
	} else if raw != "" {
		return finish(fresh, raw)
	}
	shape, err := ordinaryRecoveryShape(fresh.Metadata())
	if err != nil {
		return nil, "", err
	}
	encoded, err := json.Marshal(nativeBackfillBinding{Owner: owner, TableUUID: fresh.Metadata().TableUUID().String(), ShapeSHA256: shape})
	if err != nil {
		return nil, "", err
	}
	guarded := &ordinaryBarrierCatalog{Catalog: i.catalog, base: fresh.Metadata()}
	bound := table.New(fresh.Identifier(), fresh.Metadata(), fresh.MetadataLocation(), func(ctx context.Context) (iceio.IO, error) { return fresh.FS(ctx) }, guarded)
	tx := bound.NewTransaction()
	if err := tx.AssertDefaultShape(); err != nil {
		return nil, "", err
	}
	if err := tx.AssertRefSnapshotID(table.MainBranch); err != nil {
		return nil, "", err
	}
	if err := tx.SetProperties(apacheiceberg.Properties{nativeBackfillBindingProperty: string(encoded)}); err != nil {
		return nil, "", err
	}
	if _, err := tx.Commit(ctx); err != nil {
		nativeExecutionMutationError(i.config, err)
		if state, ok := ctx.Value(nativeTableLockKey{}).(*nativeTableState); ok {
			state.invalidate(!nativeMutationOutcomeKnown(err))
		}
		return nil, "", fmt.Errorf("publish immutable backfill table binding: %w", err)
	}
	confirmed, err := i.catalog.LoadTable(ctx, fresh.Identifier())
	if err != nil {
		return nil, "", err
	}
	if confirmed.Properties()[nativeBackfillBindingProperty] != string(encoded) {
		return nil, "", errors.New("immutable backfill table binding publication was not confirmed")
	}
	var journal nativeBackfillBinding
	if err := decodeBackfillProperty(string(encoded), &journal); err != nil {
		return nil, "", err
	}
	if err := validateBackfillBinding(confirmed, journal, owner); err != nil {
		return nil, "", err
	}
	return finish(confirmed, string(encoded))
}

func nativeRecoveryTableIdle(cat catalog.Catalog, ident table.Identifier) error {
	local := nativeTableEntry(cat, ident)
	local.mu.Lock()
	defer local.mu.Unlock()
	if err := local.usable(); err != nil {
		return err
	}
	if local.sessions != 0 || local.resync != nil {
		return errors.New("ordinary recovery requires all previous local writers to be closed")
	}
	return nil
}

func validateOriginalBackfillWriter(ctx context.Context, state *nativeTableState, tbl *table.Table, config *Config, stream types.StreamInterface, threadID string) error {
	checkpoint, controlled := types.BackfillCheckpointFromContext(ctx)
	if !controlled {
		return nil
	}
	raw := tbl.Properties()[nativeBackfillBindingProperty]
	if raw == "" {
		return errors.New("controlled backfill writer requires an immutable table binding before data work")
	}
	if state.backfillBindingRaw != "" && state.backfillBindingRaw != raw {
		return errors.New("immutable backfill table binding changed while a writer was active")
	}
	if state.backfillBinding == nil || state.backfillBindingRaw != raw || state.backfillCheckpoint != checkpoint {
		var stored nativeBackfillBinding
		if err := decodeBackfillProperty(raw, &stored); err != nil {
			return err
		}
		expected, err := backfillOwner(checkpoint, stream, stored.Owner.Plan)
		if err != nil {
			return err
		}
		expected.RequestedShapeSHA256, err = requestedBackfillShape(config, stream)
		if err != nil {
			return err
		}
		external, err := readBackfillBindingJournal(ctx, stream.ID())
		if err != nil {
			return err
		}
		if err := validateBackfillBindingWithAuthority(tbl, stored, *expected, external != nil); err != nil {
			return err
		}
		if external == nil {
			return errors.New("controlled backfill writer requires durable binding evidence before data work")
		}
		if err := checkBackfillBindingJournal(external, tbl, stored.Owner.Plan); err != nil {
			return err
		}
		plan, err := decodeBackfillPlan(stored.Owner.Plan)
		if err != nil {
			return err
		}
		state.backfillBinding, state.backfillBindingRaw, state.backfillCheckpoint = &stored, raw, checkpoint
		state.backfillChunkIDs = backfillPlanIDs(stream, plan)
	}
	if _, planned := state.backfillChunkIDs[threadID]; !planned {
		return errors.New("controlled backfill writer chunk is outside the exact immutable plan")
	}
	return validateBackfillBinding(tbl, *state.backfillBinding, state.backfillBinding.Owner)
}

func readBackfillBindingJournal(ctx context.Context, stream string) (*types.BackfillBindingJournalEntry, error) {
	journal, ok := types.BackfillBindingJournalFromContext(ctx)
	if !ok || journal.Read == nil || journal.Create == nil {
		return nil, errors.New("controlled backfill requires a durable immutable binding journal")
	}
	entry, err := journal.Read(stream)
	if err != nil || entry == nil {
		return entry, err
	}
	if entry.TableUUID == "" || len(entry.ShapeSHA256) != 64 {
		return nil, errors.New("durable backfill binding journal has an invalid table binding")
	}
	plan, err := decodeBackfillPlan(string(entry.Plan))
	if err != nil || plan == nil || plan.Len() == 0 {
		return nil, errors.New("durable backfill binding journal has no exact full chunk plan")
	}
	return entry, nil
}

func checkBackfillBindingJournal(entry *types.BackfillBindingJournalEntry, tbl *table.Table, plan string) error {
	if entry == nil {
		return nil
	}
	shape, err := ordinaryRecoveryShape(tbl.Metadata())
	if err != nil {
		return err
	}
	if entry.TableUUID != tbl.Metadata().TableUUID().String() || entry.ShapeSHA256 != shape || string(entry.Plan) != plan {
		return errors.New("durable backfill binding journal differs from table UUID, shape or exact plan")
	}
	return nil
}

func persistBackfillBindingJournal(ctx context.Context, stream string, tbl *table.Table, plan string) error {
	entry, err := readBackfillBindingJournal(ctx, stream)
	if err != nil {
		return err
	}
	if err := checkBackfillBindingJournal(entry, tbl, plan); err != nil {
		return err
	}
	shape, err := ordinaryRecoveryShape(tbl.Metadata())
	if err != nil {
		return err
	}
	expected := &types.BackfillBindingJournalEntry{TableUUID: tbl.Metadata().TableUUID().String(), ShapeSHA256: shape, Plan: json.RawMessage(plan)}
	journal, _ := types.BackfillBindingJournalFromContext(ctx)
	if err := journal.Create(stream, expected); err != nil {
		return err
	}
	confirmed, err := readBackfillBindingJournal(ctx, stream)
	if err != nil {
		return err
	}
	if confirmed == nil {
		return errors.New("durable backfill binding journal publication was not confirmed")
	}
	return checkBackfillBindingJournal(confirmed, tbl, plan)
}
