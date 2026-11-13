package iceberg

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	apacheiceberg "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/google/uuid"
)

const (
	resyncIDProperty         = "olake.resync.id"
	resyncCheckpointProperty = "olake.resync.checkpoint"
	resyncBaselineProperty   = "olake.resync.baseline-sha256"
	resyncGenerationProperty = "olake.resync.generation"
	resyncSnapshotProperty   = "olake.resync.snapshot-id"
	resyncLaunchProperty     = "olake.resync.launch-id"
)

// A stage is catalog-private, not a second named table or a renamed replacement.
// Original FileIO (including REST-vended credentials) and all table IDs survive.
// mu also fences abort against a queued stage commit or publication.
type resyncControl struct {
	mu           sync.Mutex
	id           string
	launch       string
	recoverOnly  bool
	deadline     time.Time
	baselineHash string
	base         *table.Table
	stage        *nativeResyncCatalog
	published    bool
	aborted      bool
	unknown      bool
	failed       bool
	owned        map[string]struct{}
	sessions     int
	checkpoint   string
	tableUUID    string
	snapshotID   int64
}

type nativeResyncCatalog struct {
	catalog.Catalog
	attempt  *resyncControl
	metadata table.Metadata
	updates  []table.Update
	tables   nativeTableCoordinator
}

func (s *nativeResyncCatalog) target(ident table.Identifier) bool {
	return slices.Equal(ident, s.attempt.base.Identifier())
}

func (s *nativeResyncCatalog) table() *table.Table {
	return table.New(s.attempt.base.Identifier(), s.metadata, s.attempt.base.MetadataLocation(), s.fileIO, s)
}

func (s *nativeResyncCatalog) fileIO(ctx context.Context) (iceio.IO, error) {
	fs, err := s.attempt.base.FS(nativeCatalogContext(ctx, s.Catalog))
	if err != nil {
		return nil, err
	}
	return &nativeResyncIO{IO: fs, attempt: s.attempt, ctx: ctx}, nil
}

func (s *nativeResyncCatalog) LoadTable(ctx context.Context, ident table.Identifier) (*table.Table, error) {
	if !s.target(ident) {
		return s.Catalog.LoadTable(ctx, ident)
	}
	s.attempt.mu.Lock()
	defer s.attempt.mu.Unlock()
	if err := s.attempt.active(ctx); err != nil {
		return nil, err
	}
	return s.table(), nil
}

func (s *nativeResyncCatalog) CommitTable(ctx context.Context, ident table.Identifier, requirements []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	if !s.target(ident) {
		return s.Catalog.CommitTable(ctx, ident, requirements, updates)
	}
	s.attempt.mu.Lock()
	defer s.attempt.mu.Unlock()
	if err := s.attempt.active(ctx); err != nil {
		return nil, "", err
	}
	for _, requirement := range requirements {
		if err := requirement.Validate(s.metadata); err != nil {
			return nil, "", err
		}
	}
	next, err := table.UpdateTableMetadata(s.metadata, updates, "")
	if err != nil {
		return nil, "", err
	}
	if next.TableUUID() != s.attempt.base.Metadata().TableUUID() || next.Location() != s.attempt.base.Location() {
		return nil, "", errors.New("resync cannot change the table UUID or location")
	}
	s.metadata = next
	s.updates = append(s.updates, updates...)
	return next, s.attempt.base.MetadataLocation(), nil
}

func (r *resyncControl) active(ctx context.Context) error {
	if err := utils.CheckExecutionFence(ctx); err != nil {
		return err
	}
	return r.activeLocal(ctx)
}

// activeLocal requires mu and guards private I/O without reading the remote
// execution fence. Public catalog mutations still use active and wire fences.
func (r *resyncControl) activeLocal(ctx context.Context) error {
	if r.failed || r.aborted || r.published || r.unknown {
		return errors.New("resync attempt is no longer writable; retain uncertain files and start a fresh runtime")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !time.Now().Before(r.deadline) {
		return errors.New("atomic resync exceeded its 12-hour attempt deadline")
	}
	return nil
}

func resyncHash(value []byte) string {
	hash := sha256.Sum256(value)
	return hex.EncodeToString(hash[:])
}

func (i *Iceberg) PrepareResync(ctx context.Context, stream types.StreamInterface, operationID string, baseline *types.State) (*types.State, error) {
	i.resyncMu.Lock()
	defer i.resyncMu.Unlock()
	if err := utils.CheckExecutionFence(ctx); err != nil {
		return nil, err
	}
	if strings.TrimSpace(operationID) == "" {
		return nil, errors.New("resync operation ID must not be blank")
	}
	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) > destination.ResyncMaxDuration || !time.Now().Before(deadline) {
		return nil, errors.New("atomic resync requires a live deadline no more than 12 hours away")
	}
	if i.resync != nil {
		return nil, errors.New("a resync has already been prepared on this destination")
	}
	if baseline == nil {
		return nil, errors.New("atomic resync requires the full baseline checkpoint")
	}
	encoded, err := json.Marshal(baseline)
	if err != nil {
		return nil, err
	}
	canonical, err := canonicalResyncCheckpoint(string(encoded), "")
	if err != nil {
		return nil, err
	}
	ident := catalog.ToIdentifier(stream.GetDestinationDatabase(&i.config.IcebergDatabase), stream.GetDestinationTable())
	state := nativeTableEntry(i.catalog, ident)
	state.mu.Lock()
	defer state.mu.Unlock()
	options, _ := types.ResyncFromContext(ctx)
	if options == nil {
		options = &types.ResyncOptions{}
	}
	original, err := i.catalog.LoadTable(context.WithValue(ctx, nativeTableLockKey{}, state), ident)
	if err != nil {
		if options.RecoverOnly {
			return nil, fmt.Errorf("UNKNOWN: recovery cannot find the durable publication table: %w", err)
		}
		return nil, fmt.Errorf("load original resync table: %w", err)
	}
	attempt := &resyncControl{id: operationID, launch: options.LaunchID, recoverOnly: options.RecoverOnly, deadline: deadline, baselineHash: resyncHash(canonical), base: original}
	i.resync = attempt
	if original.Properties()[resyncIDProperty] == operationID {
		stored := original.Properties()[resyncBaselineProperty]
		publishedCheckpoint, err := canonicalResyncCheckpoint(original.Properties()[resyncCheckpointProperty], "")
		if err != nil {
			return nil, err
		}
		if stored == "" || stored != attempt.baselineHash && resyncHash(publishedCheckpoint) != attempt.baselineHash {
			return nil, errors.New("resync recovery checkpoint differs from the original baseline and published checkpoint")
		}
		return i.acceptPublishedResync(original.Metadata(), "")
	}
	if options.RecoverOnly {
		return nil, errors.New("UNKNOWN: recovery-only resync has no matching durable publication marker")
	}
	if state.resync != nil {
		return nil, errors.New("another resync attempt already owns the destination table")
	}
	if state.sessions != 0 {
		return nil, errors.New("close every target writer successfully before preparing resync")
	}
	ctx, finish, err := beginNativeWriterWork(ctx, i.config)
	if err != nil {
		return nil, err
	}
	defer finish()
	stage := &nativeResyncCatalog{Catalog: i.catalog, attempt: attempt, metadata: original.Metadata()}
	attempt.stage = stage
	state.resync = attempt
	// Keep history and next-row-id, but start from an explicit empty snapshot.
	snapshot, err := newNativeResyncSnapshot(ctx, attempt, nil)
	if err != nil {
		attempt.failed = true
		return nil, err
	}
	removals := []string{nativeRecoveryProperty, resyncIDProperty, resyncCheckpointProperty, resyncBaselineProperty, resyncGenerationProperty, resyncSnapshotProperty, resyncLaunchProperty}
	updates := []table.Update{table.NewAddSnapshotUpdate(snapshot), nativeResyncSetMain(original.Metadata(), snapshot.SnapshotID), table.NewRemovePropertiesUpdate(removals), table.NewSetPropertiesUpdate(apacheiceberg.Properties{resyncGenerationProperty: operationID})}
	stage.metadata, err = table.UpdateTableMetadata(stage.metadata, updates, "")
	if err != nil {
		attempt.failed = true
		return nil, err
	}
	stage.updates = append(stage.updates, updates...)
	return nil, nil
}

func (i *Iceberg) PublishResync(ctx context.Context, checkpoint *types.State) error {
	i.resyncMu.Lock()
	defer i.resyncMu.Unlock()
	if options, ok := types.ResyncFromContext(ctx); ok && options.RecoverOnly {
		return errors.New("recovery-only resync cannot publish replacement data")
	}
	r := i.resync
	if r == nil {
		return errors.New("atomic resync has not been prepared")
	}
	if r.recoverOnly {
		return errors.New("recovery-only resync cannot publish replacement data")
	}
	if checkpoint == nil {
		return errors.New("atomic resync requires the full final checkpoint")
	}
	encoded, err := json.Marshal(checkpoint)
	if err != nil {
		return err
	}
	canonical, err := canonicalResyncCheckpoint(string(encoded), "")
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.published {
		_, err := canonicalResyncCheckpoint(r.checkpoint, string(canonical))
		return err
	}
	if r.stage == nil {
		return errors.New("atomic resync has no writable prepared stage")
	}
	if err := r.active(ctx); err != nil {
		return err
	}
	if err := nativeExecutionWritable(ctx, i.config); err != nil {
		return err
	}
	ctx, cancelDeadline := context.WithDeadline(ctx, r.deadline)
	defer cancelDeadline()
	ctx, finish, err := beginNativeWriterWork(ctx, i.config)
	if err != nil {
		return err
	}
	defer finish()
	if r.sessions != 0 {
		return errors.New("close every target writer successfully before publishing resync")
	}
	state := nativeTableEntry(i.catalog, r.base.Identifier())
	state.mu.Lock()
	defer state.mu.Unlock()
	publishCtx := context.WithValue(ctx, nativeTableLockKey{}, state)
	current, err := i.catalog.LoadTable(publishCtx, r.base.Identifier())
	if err != nil {
		return err
	}
	if err := assertNativeResyncUnchanged(r.base.Metadata(), current.Metadata()); err != nil {
		return err
	}
	tip := r.stage.metadata.CurrentSnapshot()
	if tip == nil || tip.SnapshotID <= 0 {
		return errors.New("resync replacement requires a positive snapshot ID, including empty results")
	}
	snapshot, err := newNativeResyncSnapshot(publishCtx, r, tip)
	if err != nil {
		return err
	}
	updates := nativeResyncDefinitionUpdates(r.stage.updates)
	updates = append(updates, table.NewAddSnapshotUpdate(snapshot), nativeResyncSetMain(r.base.Metadata(), snapshot.SnapshotID), table.NewSetPropertiesUpdate(apacheiceberg.Properties{
		resyncIDProperty: r.id, resyncGenerationProperty: r.id, resyncCheckpointProperty: string(canonical), resyncBaselineProperty: r.baselineHash, resyncSnapshotProperty: strconv.FormatInt(snapshot.SnapshotID, 10), resyncLaunchProperty: r.launch,
	}))
	// The original metadata is the only public commit base. No private snapshots,
	// snapshot-log entries, refs or statistics are transplanted into public history.
	_, _, publishErr := i.catalog.CommitTable(publishCtx, r.base.Identifier(), nativeResyncRequirements(r.base.Metadata()), updates)
	if publishErr != nil && errors.Is(publishErr, table.ErrCommitFailed) {
		return publishErr
	}
	// A lost response is not a rejection. Retain every file and reconcile using a
	// fresh, uncancelled catalog load before claiming a durable receipt.
	r.unknown = true
	statusCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	status, statusErr := i.catalog.LoadTable(context.WithValue(statusCtx, nativeTableLockKey{}, state), r.base.Identifier())
	if statusErr == nil {
		var recovered *types.State
		recovered, statusErr = i.acceptPublishedResync(status.Metadata(), string(canonical))
		if statusErr == nil && recovered != nil {
			r.unknown = false
			state.latest = status
			state.generation++
			return nil
		}
	}
	nativeExecutionUnknown(i.config)
	return fmt.Errorf("atomic resync publication is unconfirmed; retry the same resync ID to recover: %w", errors.Join(publishErr, statusErr, errors.New("no matching durable publication receipt")))
}

func (i *Iceberg) AbortResync(ctx context.Context) error {
	i.resyncMu.Lock()
	defer i.resyncMu.Unlock()
	r := i.resync
	if r == nil {
		return errors.New("atomic resync destination has not been prepared")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.published {
		return errors.New("cannot abort an already published atomic resync")
	}
	if r.unknown {
		return errors.New("resync publication outcome unknown; retain all attempt files")
	}
	if r.recoverOnly {
		return errors.New("recovery-only resync cannot mutate attempt state")
	}
	if options, ok := types.ResyncFromContext(ctx); ok && options.RecoverOnly {
		return errors.New("recovery-only resync cannot mutate attempt state")
	}
	// Retain the tombstone: queued writers can never revive this attempt.
	r.aborted = true
	return nil
}

func (i *Iceberg) ResyncPublished() bool {
	i.resyncMu.Lock()
	defer i.resyncMu.Unlock()
	return i.resync != nil && i.resync.published
}

func (i *Iceberg) ResyncReceipt() (string, int64) {
	i.resyncMu.Lock()
	defer i.resyncMu.Unlock()
	if i.resync == nil || !i.resync.published {
		return "", 0
	}
	return i.resync.tableUUID, i.resync.snapshotID
}

func (i *Iceberg) acceptPublishedResync(metadata table.Metadata, expected string) (*types.State, error) {
	r := i.resync
	props := metadata.Properties()
	if props[resyncIDProperty] != r.id {
		return nil, nil
	}
	if r.launch != "" && props[resyncLaunchProperty] != r.launch {
		return nil, errors.New("resync durable marker belongs to a different admitted launch")
	}
	snapshot, err := strconv.ParseInt(props[resyncSnapshotProperty], 10, 64)
	if err != nil || snapshot <= 0 || props[resyncGenerationProperty] != r.id || metadata.TableUUID() == uuid.Nil {
		return nil, errors.New("published atomic resync is missing a valid UUID, generation or durable snapshot receipt")
	}
	canonical, err := canonicalResyncCheckpoint(props[resyncCheckpointProperty], expected)
	if err != nil {
		return nil, err
	}
	state := &types.State{RWMutex: &sync.RWMutex{}}
	decoder := json.NewDecoder(bytes.NewReader(canonical))
	decoder.UseNumber()
	if err := decoder.Decode(state); err != nil {
		return nil, err
	}
	if state.Type != types.GlobalType && state.Type != types.StreamType && state.Type != types.MixedType {
		return nil, errors.New("published atomic resync checkpoint has no valid state type")
	}
	r.published, r.checkpoint, r.tableUUID, r.snapshotID = true, string(canonical), metadata.TableUUID().String(), snapshot
	return state, nil
}

func nativeResyncDefinitionUpdates(updates []table.Update) []table.Update {
	result := make([]table.Update, 0, len(updates))
	for _, update := range updates {
		switch update.Action() {
		case table.UpdateAddSnapshot, table.UpdateSetSnapshotRef, table.UpdateRemoveSnapshotRef, table.UpdateRemoveSnapshots, table.UpdateSetStatistics, table.UpdateRemoveStatistics, table.UpdateSetPartitionStatistics, table.UpdateRemovePartitionStatistics:
			continue
		}
		result = append(result, update)
	}
	return result
}

func nativeResyncRequirements(base table.Metadata) []table.Requirement {
	requirements := []table.Requirement{table.AssertTableUUID(base.TableUUID()), table.AssertLastAssignedFieldID(base.LastColumnID()), table.AssertCurrentSchemaID(base.CurrentSchema().ID), table.AssertDefaultSpecID(base.DefaultPartitionSpec()), table.AssertDefaultSortOrderID(base.DefaultSortOrder())}
	if id := base.LastPartitionSpecID(); id != nil {
		requirements = append(requirements, table.AssertLastAssignedPartitionID(*id))
	}
	for name, ref := range base.Refs() {
		id := ref.SnapshotID
		requirements = append(requirements, table.AssertRefSnapshotID(name, &id))
	}
	if base.CurrentSnapshot() == nil {
		requirements = append(requirements, table.AssertRefSnapshotID(table.MainBranch, nil))
	}
	return requirements
}

func assertNativeResyncUnchanged(base, current table.Metadata) error {
	// Includes properties, all refs/history/statistics, UUID/location and V3
	// counters. A stage may not silently erase a concurrent metadata-only edit.
	if !base.Equals(current) {
		return fmt.Errorf("concurrent table identity, data, schema, properties or history change during resync: %w", table.ErrCommitFailed)
	}
	return nil
}

func (i *Iceberg) FinishResync(ctx context.Context) error {
	if native, ok := i.catalog.(*nativeCatalog); ok {
		return native.Close()
	}
	return nil
}

func nativeResyncSetMain(metadata table.Metadata, snapshotID int64) table.Update {
	ref := metadata.Ref()
	var maxRefAge, maxSnapshotAge int64
	var minSnapshots int
	if ref.MaxRefAgeMs != nil {
		maxRefAge = *ref.MaxRefAgeMs
	}
	if ref.MaxSnapshotAgeMs != nil {
		maxSnapshotAge = *ref.MaxSnapshotAgeMs
	}
	if ref.MinSnapshotsToKeep != nil {
		minSnapshots = *ref.MinSnapshotsToKeep
	}
	return table.NewSetSnapshotRefUpdate(table.MainBranch, snapshotID, table.BranchRef, maxRefAge, maxSnapshotAge, minSnapshots)
}
