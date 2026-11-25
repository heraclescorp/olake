package iceberg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/hadoop"
	"github.com/apache/iceberg-go/table"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/google/uuid"
	"io"
	"strings"
)

func encodeBackfillPlan(stream types.StreamInterface, plan *types.Set[types.Chunk]) (string, error) {
	if plan == nil {
		return "", nil
	}
	chunks := plan.Array()
	sort.Slice(chunks, func(a, b int) bool {
		return fmt.Sprintf("%s_min[%v]-max[%v]", stream.ID(), chunks[a].Min, chunks[a].Max) < fmt.Sprintf("%s_min[%v]-max[%v]", stream.ID(), chunks[b].Min, chunks[b].Max)
	})
	encoded, err := json.Marshal(chunks)
	return string(encoded), err
}

func decodeBackfillPlan(encoded string) (*types.Set[types.Chunk], error) {
	if encoded == "" {
		return nil, nil
	}
	var chunks []types.Chunk
	decoder := json.NewDecoder(strings.NewReader(encoded))
	decoder.UseNumber()
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&chunks); err != nil {
		return nil, err
	}
	if err := decoder.Decode(new(any)); err != io.EOF || len(chunks) == 0 {
		return nil, errors.New("backfill recovery ownership has an invalid full chunk plan")
	}
	return types.NewSet(chunks...), nil
}

type backfillRecoveryCreationKey struct{}

func nativeRecoveryCatalogSupported(config *Config, cat catalog.Catalog) bool {
	if config == nil {
		return false
	}
	if config.CatalogType == RestCatalog {
		return true
	}
	if wrapped, ok := cat.(*nativeCatalog); ok {
		cat = wrapped.Catalog
	}
	_, local := cat.(*hadoop.Catalog)
	return local
}

func backfillTableBinding(tbl *table.Table, stream types.StreamInterface) (*types.BackfillTableBinding, error) {
	if raw := tbl.Properties()[nativeBackfillBindingProperty]; raw != "" {
		if err := validateBackfillTableFormat(tbl, stream); err != nil {
			return nil, err
		}
		var stored nativeBackfillBinding
		if err := decodeBackfillProperty(raw, &stored); err != nil {
			return nil, err
		}
		binding := &types.BackfillTableBinding{TableUUID: stored.TableUUID, ShapeSHA256: stored.ShapeSHA256}
		if err := validateBackfillTableBinding(tbl.Metadata(), *binding); err != nil {
			return nil, err
		}
		return binding, nil
	}
	return currentBackfillTableBinding(tbl, stream)
}

func currentBackfillTableBinding(tbl *table.Table, stream types.StreamInterface) (*types.BackfillTableBinding, error) {
	if err := validateBackfillTableFormat(tbl, stream); err != nil {
		return nil, err
	}
	shape, err := ordinaryRecoveryShape(tbl.Metadata())
	if err != nil {
		return nil, err
	}
	return &types.BackfillTableBinding{TableUUID: tbl.Metadata().TableUUID().String(), ShapeSHA256: shape}, nil
}

func validateBackfillTableFormat(tbl *table.Table, stream types.StreamInterface) error {
	if tbl.Metadata().TableUUID() == uuid.Nil || tbl.Metadata().Version() < 2 {
		return errors.New("backfill table has no valid native UUID or format")
	}
	if !stream.Self().CopyOnWriteEnabled() && stream.GetUpdateType() == types.UpdateTypeDeletionVector && tbl.Metadata().Version() < 3 {
		return errors.New("backfill recovery cannot upgrade the frozen table format")
	}
	return nil
}

func (i *Iceberg) PrepareBackfillTable(ctx context.Context, stream types.StreamInterface) (*types.BackfillTableBinding, error) {
	if i.config == nil || stream == nil {
		return nil, errors.New("backfill table preparation requires a native catalog and stream")
	}
	if options, recovery := types.OrdinaryRecoveryFromContext(ctx); recovery {
		i.resyncMu.Lock()
		defer i.resyncMu.Unlock()
		r := i.backfillRecovery[stream.ID()]
		if r == nil || r.marker.Origin != options.Origin || r.marker.SourceIdentity != options.SourceIdentity {
			return nil, errors.New("recovery tables must be prepared with the complete selected catalog")
		}
		ident := catalog.ToIdentifier(stream.GetDestinationDatabase(&i.config.IcebergDatabase), stream.GetDestinationTable())
		tbl, err := i.catalog.LoadTable(ctx, ident)
		if err != nil {
			return nil, err
		}
		if err := r.validate(tbl); err != nil {
			return nil, err
		}
		if err := r.previousMarker(tbl); err != nil {
			return nil, err
		}
		return backfillTableBinding(tbl, stream)
	}
	i.resyncMu.Lock()
	defer i.resyncMu.Unlock()
	var owner *nativeBackfillOwner
	var external *types.BackfillBindingJournalEntry
	var savedCandidate *types.BackfillTableBinding
	savedAuthority := false
	if checkpoint, controlled := types.BackfillCheckpointFromContext(ctx); controlled {
		var err error
		owner, err = backfillOwner(checkpoint, stream, i.initialBackfillPlans[stream.ID()])
		if err != nil {
			return nil, err
		}
		owner.RequestedShapeSHA256, err = requestedBackfillShape(i.config, stream)
		if err != nil {
			return nil, err
		}
		savedCandidate, savedAuthority, err = savedBackfillAuthority(checkpoint, stream, owner.Plan)
		if err != nil {
			return nil, err
		}
		external, err = readBackfillBindingJournal(ctx, stream.ID())
		if err != nil {
			return nil, err
		}
		if external != nil && string(external.Plan) != owner.Plan {
			return nil, errors.New("original backfill plan differs from the durable binding journal")
		}
		if external != nil && savedCandidate != nil && (external.TableUUID != savedCandidate.TableUUID || external.ShapeSHA256 != savedCandidate.ShapeSHA256) {
			return nil, errors.New("saved scheduled backfill binding differs from the durable journal")
		}
		ctx = context.WithValue(ctx, backfillRecoveryCreationKey{}, owner)
	}
	ident := catalog.ToIdentifier(stream.GetDestinationDatabase(&i.config.IcebergDatabase), stream.GetDestinationTable())
	state := nativeTableEntry(i.catalog, ident)
	state.mu.Lock()
	defer state.mu.Unlock()
	if err := state.usable(); err != nil {
		return nil, err
	}
	if state.sessions != 0 || state.resync != nil {
		return nil, errors.New("backfill table binding must be captured before opening writers")
	}
	ctx = nativeCatalogContext(context.WithValue(ctx, nativeTableLockKey{}, state), i.catalog)
	ctx, finish, err := beginNativeWriterWork(ctx, i.config)
	if err != nil {
		return nil, err
	}
	defer finish()
	var tbl *table.Table
	if external != nil || savedCandidate != nil {
		tbl, err = i.catalog.LoadTable(ctx, ident)
	} else {
		tbl, err = loadOrCreateNativeTable(ctx, i.config, stream, i.catalog)
	}
	if err != nil {
		return nil, err
	}
	if owner != nil {
		tbl, _, err = i.bindBackfillTable(ctx, stream, tbl, *owner, savedCandidate, savedAuthority)
		if err != nil {
			return nil, err
		}
		state.latest = tbl
	}
	return backfillTableBinding(tbl, stream)
}

func backfillPlanIDs(stream types.StreamInterface, plan *types.Set[types.Chunk]) map[string]struct{} {
	if plan == nil {
		return nil
	}
	ids := make(map[string]struct{}, plan.Len())
	for _, chunk := range plan.Array() {
		ids[fmt.Sprintf("%s_min[%v]-max[%v]", stream.ID(), chunk.Min, chunk.Max)] = struct{}{}
	}
	return ids
}

// GetBackfillCommittedIDs reads a fresh table once, without opening a writer or
// publishing state. Neither the local table cache nor saved pending chunks prove
// that a chunk committed to this exact owned table and immutable plan.
func (i *Iceberg) GetBackfillCommittedIDs(ctx context.Context, stream types.StreamInterface, plan *types.Set[types.Chunk], expected types.BackfillTableBinding) (map[string]struct{}, error) {
	i.resyncMu.Lock()
	defer i.resyncMu.Unlock()
	execution, controlled := utils.ExecutionFromContext(ctx)
	if !controlled || execution.ID == "" || execution.Fence == "" || execution.Command != "sync" {
		return nil, errors.New("backfill committed IDs require a controlled sync execution")
	}
	if err := utils.CheckExecutionFence(ctx); err != nil {
		return nil, err
	}
	_, resync := types.ResyncFromContext(ctx)
	if resync || i.resync != nil || i.ordinaryRecovery != nil || i.config == nil || i.catalog == nil || stream == nil || plan == nil || plan.Len() == 0 {
		return nil, errors.New("backfill committed IDs require an exact prepared native backfill plan")
	}
	encoded, err := encodeBackfillPlan(stream, plan)
	if err != nil {
		return nil, err
	}
	ids := backfillPlanIDs(stream, plan)
	if len(ids) != plan.Len() {
		return nil, errors.New("backfill recovery chunk identities are not unique")
	}
	var owner *nativeBackfillOwner
	var recovery *ordinaryRecoveryTable
	if _, ok := types.OrdinaryRecoveryFromContext(ctx); ok {
		options, current, err := ordinaryRecoveryContext(ctx)
		if err != nil {
			return nil, err
		}
		recovery = i.backfillRecovery[stream.ID()]
		if !options.Origin.Backfill || options.BackfillComplete || options.BackfillCheckpoint == nil || options.BackfillCheckpoint.ServerIdentity != options.SourceIdentity || recovery == nil || !recovery.externalBound || recovery.marker.ExecutionID != current.ID || recovery.marker.LaunchID != current.LaunchID || recovery.marker.Origin != options.Origin || recovery.marker.SourceIdentity != options.SourceIdentity || recovery.marker.BackfillPlan != encoded {
			return nil, errors.New("backfill committed IDs differ from the prepared recovery origin, execution or exact plan")
		}
		owner = recoveryBackfillOwner(options, stream, recovery.marker.LowerLSN, encoded)
	} else {
		launchID := execution.LaunchID
		if launchID == "" {
			launchID = execution.ID
		}
		checkpoint, ok := types.BackfillCheckpointFromContext(ctx)
		if !ok || checkpoint.ExecutionID != execution.ID || checkpoint.LaunchID != launchID || i.initialBackfillPlans[stream.ID()] != encoded {
			return nil, errors.New("backfill committed IDs require the original checkpoint and exact prepared plan")
		}
		owner, err = backfillOwner(checkpoint, stream, encoded)
		if err != nil {
			return nil, err
		}
		candidate, _, err := savedBackfillAuthority(checkpoint, stream, encoded)
		if err != nil {
			return nil, err
		}
		if candidate != nil && *candidate != expected {
			return nil, errors.New("backfill committed IDs differ from the saved table binding")
		}
	}
	owner.RequestedShapeSHA256, err = requestedBackfillShape(i.config, stream)
	if err != nil {
		return nil, err
	}
	external, err := readBackfillBindingJournal(ctx, stream.ID())
	if err != nil {
		return nil, err
	}
	if external == nil {
		return nil, errors.New("backfill committed IDs require durable immutable binding evidence")
	}
	ident := catalog.ToIdentifier(stream.GetDestinationDatabase(&i.config.IcebergDatabase), stream.GetDestinationTable())
	state := nativeTableEntry(i.catalog, ident)
	state.mu.Lock()
	defer state.mu.Unlock()
	if err := state.usable(); err != nil {
		return nil, err
	}
	if state.sessions != 0 || state.resync != nil {
		return nil, errors.New("backfill committed IDs require all local writers and resync work to be closed")
	}
	ctx = nativeCatalogContext(context.WithValue(ctx, nativeTableLockKey{}, state), i.catalog)
	tbl, err := i.catalog.LoadTable(ctx, ident)
	if err != nil {
		return nil, err
	}
	actual, err := backfillTableBinding(tbl, stream)
	if err != nil {
		return nil, err
	}
	if *actual != expected {
		return nil, errors.New("backfill committed IDs table UUID or shape differs from the expected binding")
	}
	if err := checkBackfillBindingJournal(external, tbl, encoded); err != nil {
		return nil, err
	}
	var binding nativeBackfillBinding
	if err := decodeBackfillProperty(tbl.Properties()[nativeBackfillBindingProperty], &binding); err != nil {
		return nil, fmt.Errorf("decode immutable backfill binding: %w", err)
	}
	if err := validateBackfillBindingWithAuthority(tbl, binding, *owner, true); err != nil {
		return nil, err
	}
	if recovery != nil {
		if err := recovery.validate(tbl); err != nil {
			return nil, err
		}
		if err := recovery.previousMarker(tbl); err != nil {
			return nil, err
		}
	}
	committed := make(map[string]struct{})
	var metadata types.MetadataState
	if raw, exists := tbl.Properties()[nativeRecoveryProperty]; exists {
		if strings.TrimSpace(raw) == "null" {
			return nil, errors.New("backfill committed IDs contain null recovery metadata")
		}
		if err := decodeBackfillProperty(raw, &metadata); err != nil {
			return nil, fmt.Errorf("decode authoritative backfill committed IDs: %w", err)
		}
		for _, id := range metadata.FullRefreshCommittedIDs {
			if id == "" {
				return nil, errors.New("backfill committed IDs contain an empty chunk identity")
			}
			if _, planned := ids[id]; planned {
				committed[id] = struct{}{}
			}
		}
	}
	if len(metadata.FullRefreshCommittedIDs) == 0 && (tbl.CurrentSnapshot() != nil || len(tbl.Metadata().Snapshots()) != 0) {
		return nil, errors.New("backfill table with snapshot history has no authoritative committed chunk metadata")
	}
	if err := utils.CheckExecutionFence(ctx); err != nil {
		return nil, err
	}
	return committed, nil
}

func (i *Iceberg) PrepareBackfillRecovery(ctx context.Context, streams []types.StreamInterface, lower string) error {
	i.resyncMu.Lock()
	defer i.resyncMu.Unlock()
	options, execution, err := ordinaryRecoveryContext(ctx)
	if err != nil {
		return err
	}
	if !options.Origin.Backfill || (options.BackfillComplete && options.CheckpointLSN == "") || options.BackfillState == nil || options.BackfillCheckpoint == nil || len(streams) == 0 || i.resync != nil || i.ordinaryRecovery != nil || i.backfillRecovery != nil || !nativeRecoveryCatalogSupported(i.config, i.catalog) {
		return errors.New("backfill recovery requires an immutable candidate and unprepared native REST or Hadoop catalog")
	}
	if options.BackfillCheckpoint.ServerIdentity != options.SourceIdentity {
		return errors.New("backfill recovery candidate differs from the validated live source")
	}
	if _, err := ordinaryRecoveryLSN(lower); err != nil {
		return err
	}
	ack := uint64(0)
	if options.BackfillAcknowledgedLSN != "" {
		ack, err = ordinaryRecoveryLSN(options.BackfillAcknowledgedLSN)
		if err != nil {
			return err
		}
	}
	if options.CheckpointLSN != "" {
		checkpoint, checkpointErr := ordinaryRecoveryLSN(options.CheckpointLSN)
		lowerValue, _ := ordinaryRecoveryLSN(lower)
		if checkpointErr != nil || checkpoint < lowerValue {
			return errors.New("backfill recovery checkpoint is behind the immutable lower LSN")
		}
		if checkpoint > ack {
			ack = checkpoint
		}
	}
	controls := make(map[string]*ordinaryRecoveryTable, len(streams))
	tables := make(map[string]*table.Table, len(streams))
	owners := make(map[string]nativeBackfillOwner, len(streams))
	candidates := make(map[string]*types.BackfillTableBinding, len(streams))
	identifiers := make(map[string]struct{}, len(streams))
	for _, stream := range streams {
		if stream == nil {
			return errors.New("backfill recovery selected stream is nil")
		}
		ident := catalog.ToIdentifier(stream.GetDestinationDatabase(&i.config.IcebergDatabase), stream.GetDestinationTable())
		key, _ := json.Marshal(ident)
		if _, duplicate := identifiers[string(key)]; duplicate {
			return errors.New("backfill recovery selected streams alias the same destination table")
		}
		identifiers[string(key)] = struct{}{}
		if _, duplicate := controls[stream.ID()]; duplicate {
			return errors.New("backfill recovery selected stream is duplicated")
		}
		candidate := options.BackfillState.GetBackfillTable(stream.Self())
		plan := options.BackfillState.GetBackfillPlan(stream.Self())
		completed := options.BackfillState.HasCompletedBackfill(stream.Self())
		pending := options.BackfillState.GetChunks(stream.Self())
		if candidate == nil && (plan != nil || completed || pending != nil) {
			return fmt.Errorf("backfill recovery stream %q has progress without a saved table binding", stream.ID())
		}
		if candidate != nil && (plan == nil || plan.Len() == 0) {
			return fmt.Errorf("backfill recovery stream %q has no full immutable chunk plan", stream.ID())
		}
		external, err := readBackfillBindingJournal(ctx, stream.ID())
		if err != nil {
			return err
		}
		if external != nil {
			expected := &types.BackfillTableBinding{TableUUID: external.TableUUID, ShapeSHA256: external.ShapeSHA256}
			if candidate != nil && *candidate != *expected {
				return errors.New("candidate table binding differs from the durable binding journal")
			}
			if plan != nil {
				encoded, err := encodeBackfillPlan(stream, plan)
				if err != nil {
					return err
				}
				if encoded != string(external.Plan) {
					return errors.New("candidate full chunk plan differs from the durable binding journal")
				}
			}
			candidate = expected
			plan, err = decodeBackfillPlan(string(external.Plan))
			if err != nil {
				return err
			}
		}
		if err := nativeRecoveryTableIdle(i.catalog, ident); err != nil {
			return err
		}
		requested, err := requestedBackfillShape(i.config, stream)
		if err != nil {
			return err
		}
		r := &ordinaryRecoveryTable{marker: ordinaryRecoveryMarker{Version: 1, Origin: options.Origin, SourceIdentity: options.SourceIdentity, ExecutionID: execution.ID, LaunchID: execution.LaunchID, LowerLSN: lower, Stream: stream.ID()}, acknowledged: ack != 0}
		tbl, loadErr := i.catalog.LoadTable(ctx, ident)
		if errors.Is(loadErr, catalog.ErrNoSuchTable) && candidate == nil && ack == 0 {
			controls[stream.ID()] = r
			continue
		}
		if loadErr != nil {
			return fmt.Errorf("load backfill recovery table %q: %w", stream.ID(), loadErr)
		}
		actual, err := currentBackfillTableBinding(tbl, stream)
		if err != nil {
			return err
		}
		if candidate != nil {
			if err := validateBackfillTableBinding(tbl.Metadata(), *candidate); err != nil {
				return fmt.Errorf("backfill recovery table %q UUID or shape differs from the candidate: %w", stream.ID(), err)
			}
		}
		planJSON, err := encodeBackfillPlan(stream, plan)
		if err != nil {
			return err
		}
		var journal nativeBackfillBinding
		journalRaw := tbl.Properties()[nativeBackfillBindingProperty]
		if journalRaw != "" {
			if err := decodeBackfillProperty(journalRaw, &journal); err != nil {
				return err
			}
			if candidate != nil && (journal.TableUUID != candidate.TableUUID || journal.ShapeSHA256 != candidate.ShapeSHA256) {
				return errors.New("immutable backfill binding differs from the exact candidate binding")
			}
			if planJSON != "" && planJSON != journal.Owner.Plan {
				return errors.New("immutable backfill binding plan differs from the candidate")
			}
			planJSON = journal.Owner.Plan
		} else if candidate == nil {
			var intent nativeBackfillOwner
			if err := decodeBackfillProperty(tbl.Properties()[nativeBackfillOwnerProperty], &intent); err != nil {
				return fmt.Errorf("unstarted backfill table has no valid creation ownership: %w", err)
			}
			planJSON = intent.Plan
		}
		if err := checkBackfillBindingJournal(external, tbl, planJSON); err != nil {
			return err
		}
		plan, err = decodeBackfillPlan(planJSON)
		if err != nil || plan == nil || plan.Len() == 0 {
			return errors.New("backfill recovery ownership has no full immutable chunk plan")
		}
		owner := recoveryBackfillOwner(options, stream, lower, planJSON)
		owner.RequestedShapeSHA256 = requested
		if journalRaw != "" {
			savedAuthority := candidate != nil && options.BackfillState.BackfillSource != nil && *options.BackfillState.BackfillSource == options.BackfillCheckpoint.SourceBinding()
			if err := validateBackfillBindingWithAuthority(tbl, journal, *owner, external != nil || savedAuthority); err != nil {
				return err
			}
			r.bindingEncoded = journalRaw
		} else if candidate == nil {
			var intent nativeBackfillOwner
			if err := decodeBackfillProperty(tbl.Properties()[nativeBackfillOwnerProperty], &intent); err != nil {
				return err
			}
			if err := validateBackfillOwner(intent, *owner); err != nil {
				return err
			}
			shape, err := ordinaryRecoveryShapeAtLocation(tbl.Metadata(), "")
			if err != nil || shape != requested {
				return errors.New("unbound backfill table differs from the frozen requested shape")
			}
			if err := pristineBackfillTable(tbl); err != nil {
				return err
			}
		}
		r.marker.TableUUID, r.marker.ShapeSHA256 = actual.TableUUID, actual.ShapeSHA256
		r.marker.BackfillPlan, r.chunkIDs = planJSON, backfillPlanIDs(stream, plan)
		if raw := tbl.Properties()[ordinaryRecoveryProperty]; raw != "" {
			var previous ordinaryRecoveryMarker
			if err := decodeBackfillProperty(raw, &previous); err != nil {
				return err
			}
			r.marker.BackfillTargetLSN = previous.BackfillTargetLSN
		}
		encoded, err := json.Marshal(r.marker)
		if err != nil {
			return err
		}
		r.encoded = string(encoded)
		if err := r.previousMarker(tbl); err != nil {
			return fmt.Errorf("backfill recovery table %q: %w", stream.ID(), err)
		}
		if options.BackfillComplete {
			if err := r.validateCommittedBackfillPlan(tbl); err != nil {
				return err
			}
		}
		if ack != 0 {
			if _, err := ordinaryRecoveryPosition(tbl, ack); err != nil {
				return fmt.Errorf("backfill recovery table %q cannot cover the acknowledged slot: %w", stream.ID(), err)
			}
		}
		controls[stream.ID()], tables[stream.ID()], owners[stream.ID()], candidates[stream.ID()] = r, tbl, *owner, candidate
	}
	storedTarget, err := backfillRecoveryTarget(controls)
	if err != nil {
		return err
	}
	if storedTarget != "" {
		targetValue, _ := ordinaryRecoveryLSN(storedTarget)
		lowerValue, _ := ordinaryRecoveryLSN(lower)
		for id, control := range controls {
			tbl := tables[id]
			if tbl == nil {
				return errors.New("backfill recovery table is missing after a replay target was persisted")
			}
			position, _, err := control.backfillPosition(tbl, lower, lowerValue)
			if err != nil {
				return err
			}
			value, _ := ordinaryRecoveryLSN(position)
			if value > targetValue {
				return errors.New("backfill recovery table is beyond the persisted replay target")
			}
		}
	}
	for _, stream := range streams {
		r := controls[stream.ID()]
		if tables[stream.ID()] == nil {
			continue
		}
		ident := catalog.ToIdentifier(stream.GetDestinationDatabase(&i.config.IcebergDatabase), stream.GetDestinationTable())
		state := nativeTableEntry(i.catalog, ident)
		state.mu.Lock()
		prepareErr := func() error {
			if err := state.usable(); err != nil {
				return err
			}
			if state.sessions != 0 || state.resync != nil {
				return errors.New("backfill recovery requires all previous local writers to be closed")
			}
			workCtx := nativeCatalogContext(context.WithValue(ctx, nativeTableLockKey{}, state), i.catalog)
			workCtx, finish, err := beginNativeWriterWork(workCtx, i.config)
			if err != nil {
				return err
			}
			defer finish()
			tbl, err := i.catalog.LoadTable(workCtx, ident)
			if err != nil {
				return err
			}
			if err := r.validate(tbl); err != nil {
				return err
			}
			if err := r.previousMarker(tbl); err != nil {
				return err
			}
			tbl, r.bindingEncoded, err = i.bindBackfillTable(workCtx, stream, tbl, owners[stream.ID()], candidates[stream.ID()], false)
			if err != nil {
				return err
			}
			r.externalBound = true
			state.backfillRecovery, state.latest = r, tbl
			return nil
		}()
		state.mu.Unlock()
		if prepareErr != nil {
			return fmt.Errorf("prepare backfill recovery table %q: %w", stream.ID(), prepareErr)
		}
	}
	i.backfillRecovery = controls
	return nil
}

func backfillRecoveryTarget(controls map[string]*ordinaryRecoveryTable) (string, error) {
	target := ""
	for _, control := range controls {
		if control.marker.BackfillTargetLSN == "" {
			continue
		}
		value, err := ordinaryRecoveryLSN(control.marker.BackfillTargetLSN)
		lower, lowerErr := ordinaryRecoveryLSN(control.marker.LowerLSN)
		if err != nil || lowerErr != nil || value < lower {
			return "", errors.New("backfill recovery marker contains an invalid replay target")
		}
		if target != "" && !ordinaryRecoverySameLSN(target, control.marker.BackfillTargetLSN) {
			return "", errors.New("backfill recovery tables disagree on the persisted replay target")
		}
		target = control.marker.BackfillTargetLSN
	}
	return target, nil
}

func (i *Iceberg) BackfillRecoveryTarget(ctx context.Context) (string, error) {
	i.resyncMu.Lock()
	defer i.resyncMu.Unlock()
	options, execution, err := ordinaryRecoveryContext(ctx)
	if err != nil {
		return "", err
	}
	if !options.Origin.Backfill || len(i.backfillRecovery) == 0 {
		return "", errors.New("backfill recovery tables have not been prepared")
	}
	for _, control := range i.backfillRecovery {
		if control.marker.Origin != options.Origin || control.marker.SourceIdentity != options.SourceIdentity || control.marker.ExecutionID != execution.ID {
			return "", errors.New("backfill recovery target differs from the prepared origin and execution")
		}
	}
	return backfillRecoveryTarget(i.backfillRecovery)
}

func (i *Iceberg) GetBackfillPlan(ctx context.Context, stream types.StreamInterface) (*types.Set[types.Chunk], error) {
	i.resyncMu.Lock()
	defer i.resyncMu.Unlock()
	options, execution, err := ordinaryRecoveryContext(ctx)
	if err != nil {
		return nil, err
	}
	r := i.backfillRecovery[stream.ID()]
	if !options.Origin.Backfill || options.BackfillComplete || r == nil || r.marker.ExecutionID != execution.ID || r.marker.Origin != options.Origin || r.marker.SourceIdentity != options.SourceIdentity {
		return nil, errors.New("backfill recovery plan does not match the prepared table and execution")
	}
	return decodeBackfillPlan(r.marker.BackfillPlan)
}

func (i *Iceberg) PrepareBackfillPlan(ctx context.Context, stream types.StreamInterface, plan *types.Set[types.Chunk]) error {
	i.resyncMu.Lock()
	defer i.resyncMu.Unlock()
	if _, recovery := types.OrdinaryRecoveryFromContext(ctx); !recovery {
		if _, controlled := types.BackfillCheckpointFromContext(ctx); !controlled || plan == nil || plan.Len() == 0 {
			return errors.New("original backfill plan requires a controlled checkpoint and nonempty exact plan")
		}
		encoded, err := encodeBackfillPlan(stream, plan)
		if err != nil {
			return err
		}
		if i.initialBackfillPlans == nil {
			i.initialBackfillPlans = make(map[string]string)
		}
		if previous, exists := i.initialBackfillPlans[stream.ID()]; exists && previous != encoded {
			return errors.New("original backfill immutable chunk plan changed")
		}
		i.initialBackfillPlans[stream.ID()] = encoded
		return nil
	}
	options, execution, err := ordinaryRecoveryContext(ctx)
	if err != nil {
		return err
	}
	r := i.backfillRecovery[stream.ID()]
	if !options.Origin.Backfill || options.BackfillComplete || r == nil || r.marker.ExecutionID != execution.ID || r.marker.Origin != options.Origin || r.marker.SourceIdentity != options.SourceIdentity || r.acknowledged || plan == nil || plan.Len() == 0 {
		return errors.New("backfill recovery plan does not match the prepared table and execution")
	}
	ids := backfillPlanIDs(stream, plan)
	if len(ids) != plan.Len() {
		return errors.New("backfill recovery chunk identities are not unique")
	}
	encodedPlan, err := encodeBackfillPlan(stream, plan)
	if err != nil {
		return err
	}
	if r.chunkIDs != nil {
		if encodedPlan != r.marker.BackfillPlan {
			return errors.New("backfill recovery immutable chunk plan changed")
		}
		return nil
	}
	ident := catalog.ToIdentifier(stream.GetDestinationDatabase(&i.config.IcebergDatabase), stream.GetDestinationTable())
	state := nativeTableEntry(i.catalog, ident)
	state.mu.Lock()
	defer state.mu.Unlock()
	if err := state.usable(); err != nil {
		return err
	}
	if state.sessions != 0 || state.resync != nil {
		return errors.New("backfill recovery plan must be persisted before opening writers")
	}
	ctx = nativeCatalogContext(context.WithValue(ctx, nativeTableLockKey{}, state), i.catalog)
	ctx, finish, err := beginNativeWriterWork(ctx, i.config)
	if err != nil {
		return err
	}
	defer finish()
	owner := recoveryBackfillOwner(options, stream, r.marker.LowerLSN, encodedPlan)
	owner.RequestedShapeSHA256, err = requestedBackfillShape(i.config, stream)
	if err != nil {
		return err
	}
	ctx = context.WithValue(ctx, backfillRecoveryCreationKey{}, owner)
	fresh, err := loadOrCreateNativeTable(ctx, i.config, stream, i.catalog)
	if err != nil {
		return err
	}
	confirmed, journal, err := i.bindBackfillTable(ctx, stream, fresh, *owner, nil, false)
	if err != nil {
		return err
	}
	actual, err := currentBackfillTableBinding(confirmed, stream)
	if err != nil {
		return err
	}
	r.marker.TableUUID, r.marker.ShapeSHA256 = actual.TableUUID, actual.ShapeSHA256
	r.marker.BackfillPlan = encodedPlan
	encoded, err := json.Marshal(r.marker)
	if err != nil {
		return err
	}
	r.encoded, r.chunkIDs, r.bindingEncoded = string(encoded), ids, journal
	r.externalBound = true
	state.backfillRecovery, state.latest = r, confirmed
	return nil
}
func (r *ordinaryRecoveryTable) validateCommittedBackfillPlan(tbl *table.Table) error {
	if len(r.chunkIDs) == 0 {
		return errors.New("backfill recovery cannot establish the lower LSN without the exact full chunk plan")
	}
	var metadata types.MetadataState
	if raw := tbl.Properties()[nativeRecoveryProperty]; raw != "" {
		if err := json.Unmarshal([]byte(raw), &metadata); err != nil {
			return err
		}
	}
	committed := make(map[string]struct{}, len(metadata.FullRefreshCommittedIDs))
	for _, id := range metadata.FullRefreshCommittedIDs {
		committed[id] = struct{}{}
	}
	for id := range r.chunkIDs {
		if _, ok := committed[id]; !ok {
			return errors.New("backfill recovery full chunk plan is not authoritatively committed")
		}
	}
	return nil
}

func (r *ordinaryRecoveryTable) backfillPosition(tbl *table.Table, lower string, lowerValue uint64) (string, string, error) {
	status, _ := nativeMetadataLSNPosition(tbl.Properties()[nativeRecoveryProperty])
	if status == "present" {
		position, err := ordinaryRecoveryPosition(tbl, lowerValue)
		return position, "", err
	}
	if status != "absent" && status != "no_cdc_state" {
		return "", "", errors.New("backfill recovery table contains invalid CDC metadata")
	}
	if err := r.validateCommittedBackfillPlan(tbl); err != nil {
		return "", "", err
	}
	var root map[string]json.RawMessage
	raw := tbl.Properties()[nativeRecoveryProperty]
	if raw != "" {
		if err := json.Unmarshal([]byte(raw), &root); err != nil {
			return "", "", err
		}
	}
	if root == nil {
		root = make(map[string]json.RawMessage)
	}
	state, err := json.Marshal(map[string]string{"lsn": lower})
	if err != nil {
		return "", "", err
	}
	root["state"], err = json.Marshal(string(state))
	if err != nil {
		return "", "", err
	}
	encoded, err := json.Marshal(root)
	return lower, string(encoded), err
}
