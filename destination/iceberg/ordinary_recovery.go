package iceberg

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow/array"
	apacheiceberg "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/google/uuid"
)

const ordinaryRecoveryProperty = "olake.ordinary-recovery"

type ordinaryRecoveryMarker struct {
	Version        int                          `json:"version"`
	Origin         types.OrdinaryRecoveryOrigin `json:"origin"`
	SourceIdentity string                       `json:"source_identity"`
	ExecutionID    string                       `json:"execution_id"`
	LaunchID       string                       `json:"launch_id"`
	LowerLSN       string                       `json:"lower_lsn"`
	Stream         string                       `json:"stream"`
	TableUUID      string                       `json:"table_uuid"`
	ShapeSHA256    string                       `json:"shape_sha256"`
}

type ordinaryRecoveryTable struct {
	marker   ordinaryRecoveryMarker
	encoded  string
	proof    types.OrdinaryRecoveryTableProof
	target   uint64
	prepared bool
}

func ordinaryRecoveryLSN(value string) (uint64, error) {
	parts := strings.Split(value, "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return 0, errors.New("ordinary recovery requires a valid PostgreSQL LSN")
	}
	high, highErr := strconv.ParseUint(parts[0], 16, 32)
	low, lowErr := strconv.ParseUint(parts[1], 16, 32)
	if highErr != nil || lowErr != nil {
		return 0, errors.New("ordinary recovery requires a valid PostgreSQL LSN")
	}
	return high<<32 | low, nil
}

func ordinaryRecoveryPosition(tbl *table.Table, lower uint64) (string, error) {
	status, position := nativeMetadataLSNPosition(tbl.Properties()[nativeRecoveryProperty])
	if status != "present" {
		// Backfill membership in a saved GLOBAL state does not prove that an
		// absent destination cursor covers that state's LSN. Do not invent it.
		return "", fmt.Errorf("ordinary recovery table has no authoritative CDC position (%s)", status)
	}
	value, err := ordinaryRecoveryLSN(position)
	if err != nil || value < lower {
		return "", errors.New("ordinary recovery table metadata is behind the saved baseline")
	}
	return position, nil
}

func ordinaryRecoveryShape(metadata table.Metadata) (string, error) {
	encoded, err := json.Marshal(struct {
		Version       int
		Location      string
		Schemas       []*apacheiceberg.Schema
		CurrentSchema int
		LastColumn    int
		Specs         []apacheiceberg.PartitionSpec
		DefaultSpec   int
		LastPartition *int
		Orders        []table.SortOrder
		DefaultOrder  int
	}{metadata.Version(), metadata.Location(), metadata.Schemas(), metadata.CurrentSchema().ID,
		metadata.LastColumnID(), metadata.PartitionSpecs(), metadata.DefaultPartitionSpec(),
		metadata.LastPartitionSpecID(), metadata.SortOrders(), metadata.DefaultSortOrder()})
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func ordinaryRecoveryContext(ctx context.Context) (*types.OrdinaryRecoveryOptions, *utils.ExecutionOptions, error) {
	options, ok := types.OrdinaryRecoveryFromContext(ctx)
	execution, controlled := utils.ExecutionFromContext(ctx)
	if !ok || !controlled || execution.ID == "" || execution.LaunchID == "" || execution.Fence == "" || execution.Command != "sync" {
		return nil, nil, errors.New("ordinary recovery requires a controlled sync execution")
	}
	origin := options.Origin
	if origin.Version != 1 || origin.OriginalExecutionID == "" || origin.OriginalLaunchID == "" ||
		execution.ID == origin.OriginalExecutionID || execution.ID == origin.OriginalLaunchID || execution.LaunchID != origin.OriginalLaunchID || options.SourceIdentity == "" {
		return nil, nil, errors.New("ordinary recovery requires the retired origin and a validated live source identity")
	}
	for _, value := range []string{origin.OriginalReceiptSHA256, origin.SourceSHA256, origin.DestinationSHA256, origin.CatalogSHA256, origin.BaselineStateSHA256} {
		decoded, err := hex.DecodeString(value)
		if err != nil || len(decoded) != sha256.Size || value != strings.ToLower(value) {
			return nil, nil, errors.New("ordinary recovery origin contains an invalid artifact digest")
		}
	}
	if _, resync := types.ResyncFromContext(ctx); resync {
		return nil, nil, errors.New("ordinary recovery cannot be combined with resync")
	}
	if err := utils.CheckExecutionFence(ctx); err != nil {
		return nil, nil, err
	}
	return options, execution, nil
}

func (r *ordinaryRecoveryTable) validate(tbl *table.Table) error {
	shape, err := ordinaryRecoveryShape(tbl.Metadata())
	if err != nil {
		return err
	}
	if tbl.Metadata().TableUUID().String() != r.marker.TableUUID || shape != r.marker.ShapeSHA256 {
		return errors.New("ordinary recovery table UUID or shape changed")
	}
	if r.proof.FenceSnapshotID != 0 {
		if tbl.Properties()[ordinaryRecoveryProperty] != r.encoded || !ordinaryRecoveryAncestor(tbl, r.proof.FenceSnapshotID, r.encoded) {
			return errors.New("ordinary recovery table lost its barrier marker or ancestry")
		}
	}
	return nil
}

func ordinaryRecoveryAncestor(tbl *table.Table, ancestor int64, marker string) bool {
	for snapshot, remaining := tbl.CurrentSnapshot(), len(tbl.Metadata().Snapshots()); snapshot != nil && remaining > 0; remaining-- {
		if snapshot.SnapshotID == ancestor {
			return snapshot.Summary != nil && snapshot.Summary.Operation == table.OpAppend && snapshot.Summary.Properties[ordinaryRecoveryProperty] == marker
		}
		if snapshot.ParentSnapshotID == nil {
			return false
		}
		snapshot = tbl.SnapshotByID(*snapshot.ParentSnapshotID)
	}
	return false
}

// The barrier uses the real catalog's FileIO (including REST-vended credentials),
// while adding schema requirements absent from AssertDefaultShape. Reload after
// committing restores the catalog's original Table and scan planner.
type ordinaryBarrierCatalog struct {
	catalog.Catalog
	base table.Metadata
}

func (c *ordinaryBarrierCatalog) CommitTable(ctx context.Context, ident table.Identifier, requirements []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	requirements = append(requirements, table.AssertTableUUID(c.base.TableUUID()), table.AssertCurrentSchemaID(c.base.CurrentSchema().ID), table.AssertLastAssignedFieldID(c.base.LastColumnID()))
	if id := c.base.LastPartitionSpecID(); id != nil {
		requirements = append(requirements, table.AssertLastAssignedPartitionID(*id))
	}
	return c.Catalog.CommitTable(ctx, ident, requirements, updates)
}

func (i *Iceberg) PrepareOrdinaryRecovery(ctx context.Context, streams []types.StreamInterface, lower string) ([]types.OrdinaryRecoveryTableProof, error) {
	i.resyncMu.Lock()
	defer i.resyncMu.Unlock()
	options, execution, err := ordinaryRecoveryContext(ctx)
	if err != nil {
		return nil, err
	}
	if i.config == nil || i.config.CatalogType != RestCatalog || i.resync != nil || len(streams) == 0 || i.ordinaryRecovery != nil {
		return nil, errors.New("ordinary recovery requires existing native REST/S3 Tables and an unprepared adapter")
	}
	lowerValue, err := ordinaryRecoveryLSN(lower)
	if err != nil {
		return nil, err
	}
	controls := make(map[string]*ordinaryRecoveryTable, len(streams))
	identifiers := make(map[string]struct{}, len(streams))
	// Preflight every selected table before the first mutation. Each barrier then
	// reloads under the local table lock and repeats these identity checks.
	for _, stream := range streams {
		if stream == nil {
			return nil, errors.New("ordinary recovery selected stream is nil")
		}
		ident := catalog.ToIdentifier(stream.GetDestinationDatabase(&i.config.IcebergDatabase), stream.GetDestinationTable())
		key, _ := json.Marshal(ident)
		if _, duplicate := identifiers[string(key)]; duplicate {
			return nil, errors.New("ordinary recovery selected streams alias the same destination table")
		}
		identifiers[string(key)] = struct{}{}
		if _, duplicate := controls[stream.ID()]; duplicate {
			return nil, errors.New("ordinary recovery selected stream is duplicated")
		}
		tbl, err := i.catalog.LoadTable(ctx, ident)
		if err != nil {
			return nil, fmt.Errorf("load existing ordinary recovery table %q: %w", stream.ID(), err)
		}
		if tbl.Metadata().TableUUID() == uuid.Nil || tbl.Metadata().Version() < 2 {
			return nil, errors.New("ordinary recovery table has no valid native UUID or format")
		}
		if stream.GetUpdateType() == types.UpdateTypeDeletionVector && tbl.Metadata().Version() < 3 {
			return nil, errors.New("ordinary recovery cannot upgrade the frozen table format")
		}
		position, err := ordinaryRecoveryPosition(tbl, lowerValue)
		if err != nil {
			return nil, fmt.Errorf("stream %q: %w", stream.ID(), err)
		}
		shape, err := ordinaryRecoveryShape(tbl.Metadata())
		if err != nil {
			return nil, err
		}
		marker := ordinaryRecoveryMarker{Version: 1, Origin: options.Origin, SourceIdentity: options.SourceIdentity, ExecutionID: execution.ID, LaunchID: execution.LaunchID, LowerLSN: lower, Stream: stream.ID(), TableUUID: tbl.Metadata().TableUUID().String(), ShapeSHA256: shape}
		encoded, err := json.Marshal(marker)
		if err != nil {
			return nil, err
		}
		r := &ordinaryRecoveryTable{marker: marker, encoded: string(encoded), proof: types.OrdinaryRecoveryTableProof{Stream: stream.ID(), TableUUID: marker.TableUUID, CommittedLSN: position}}
		if err := r.previousMarker(tbl); err != nil {
			return nil, err
		}
		controls[stream.ID()] = r
	}
	i.ordinaryRecovery = controls
	proofs := make([]types.OrdinaryRecoveryTableProof, 0, len(streams))
	for _, stream := range streams {
		r := controls[stream.ID()]
		if err := i.ordinaryBarrier(ctx, stream, r, lowerValue); err != nil {
			return nil, fmt.Errorf("ordinary recovery barrier %q: %w", stream.ID(), err)
		}
		proofs = append(proofs, r.proof)
	}
	target := lowerValue
	for _, proof := range proofs {
		position, _ := ordinaryRecoveryLSN(proof.CommittedLSN)
		if position > target {
			target = position
		}
	}
	for _, control := range controls {
		control.target, control.prepared = target, true
	}
	return proofs, nil
}

func (r *ordinaryRecoveryTable) previousMarker(tbl *table.Table) error {
	stored := tbl.Properties()[ordinaryRecoveryProperty]
	if stored == "" {
		return nil
	}
	var previous ordinaryRecoveryMarker
	if err := json.Unmarshal([]byte(stored), &previous); err != nil {
		return errors.New("ordinary recovery table has a malformed previous marker")
	}
	// Epochs may change, immutable evidence and source identity may not.
	previous.ExecutionID, previous.LaunchID = r.marker.ExecutionID, r.marker.LaunchID
	if previous != r.marker {
		return errors.New("ordinary recovery table marker differs from immutable origin, source identity or shape")
	}
	return nil
}

func (i *Iceberg) ordinaryBarrier(ctx context.Context, stream types.StreamInterface, r *ordinaryRecoveryTable, lower uint64) error {
	ident := catalog.ToIdentifier(stream.GetDestinationDatabase(&i.config.IcebergDatabase), stream.GetDestinationTable())
	state := nativeTableEntry(i.catalog, ident)
	state.mu.Lock()
	defer state.mu.Unlock()
	if err := state.usable(); err != nil {
		return err
	}
	if state.sessions != 0 || state.resync != nil {
		return errors.New("ordinary recovery requires all previous local writers to be closed")
	}
	ctx = nativeCatalogContext(context.WithValue(ctx, nativeTableLockKey{}, state), i.catalog)
	ctx, finish, err := beginNativeWriterWork(ctx, i.config)
	if err != nil {
		return err
	}
	defer finish()
	var conflict error
	for range nativeCommitAttempts {
		fresh, err := i.catalog.LoadTable(ctx, ident)
		if err != nil {
			return err
		}
		if err := r.validate(fresh); err != nil {
			return err
		}
		if err := r.previousMarker(fresh); err != nil {
			return err
		}
		position, err := ordinaryRecoveryPosition(fresh, lower)
		if err != nil {
			return err
		}
		previousPosition, _ := ordinaryRecoveryLSN(r.proof.CommittedLSN)
		currentPosition, _ := ordinaryRecoveryLSN(position)
		if currentPosition < previousPosition {
			return errors.New("ordinary recovery table position regressed during barrier preparation")
		}
		// Never adopt a marker as the fence: every preparation must publish a
		// new real snapshot, including interrupted or repeated recovery calls.
		barrierCatalog := &ordinaryBarrierCatalog{Catalog: i.catalog, base: fresh.Metadata()}
		bound := table.New(fresh.Identifier(), fresh.Metadata(), fresh.MetadataLocation(), func(ctx context.Context) (iceio.IO, error) { return fresh.FS(ctx) }, barrierCatalog)
		tx := bound.NewTransaction()
		if err := tx.AssertRefSnapshotID(table.MainBranch); err != nil {
			return err
		}
		if err := tx.AssertDefaultShape(); err != nil {
			return err
		}
		if err := tx.SetProperties(apacheiceberg.Properties{ordinaryRecoveryProperty: r.encoded}); err != nil {
			return err
		}
		schema, err := table.SchemaToArrowSchema(fresh.Schema(), nil, true, false)
		if err != nil {
			return err
		}
		reader, err := array.NewRecordReader(schema, nil)
		if err != nil {
			return err
		}
		err = tx.Append(ctx, reader, apacheiceberg.Properties{ordinaryRecoveryProperty: r.encoded})
		reader.Release()
		if err != nil {
			return err
		}
		committed, err := tx.Commit(ctx)
		if err != nil {
			nativeExecutionMutationError(i.config, err)
			unknown := !nativeMutationOutcomeKnown(err)
			state.invalidate(unknown)
			if unknown {
				return fmt.Errorf("UNKNOWN: ordinary recovery barrier commit is ambiguous: %w", err)
			}
			if !errors.Is(err, table.ErrCommitFailed) && !errors.Is(err, rest.ErrCommitFailed) {
				return err
			}
			conflict = err
			continue
		}
		if nativeSnapshotID(committed) == nativeSnapshotID(fresh) || !ordinaryRecoveryAncestor(committed, nativeSnapshotID(committed), r.encoded) {
			return errors.New("ordinary recovery did not create a real new append snapshot")
		}
		r.proof.FenceSnapshotID = nativeSnapshotID(committed)
		// The receipt is always based on an authoritative fresh catalog read,
		// never just the SDK's optimistic transaction result.
		confirmed, err := i.catalog.LoadTable(ctx, ident)
		if err != nil {
			return err
		}
		if err := r.validate(confirmed); err != nil {
			return err
		}
		if confirmed.Properties()[nativeRecoveryProperty] != fresh.Properties()[nativeRecoveryProperty] {
			return errors.New("ordinary recovery barrier changed the authoritative CDC metadata")
		}
		r.proof.CommittedLSN, err = ordinaryRecoveryPosition(confirmed, lower)
		if err != nil {
			return err
		}
		state.ordinaryRecovery, state.latest = r, confirmed
		return nil
	}
	return fmt.Errorf("ordinary recovery barrier conflicts exhausted: %w", conflict)
}

func (i *Iceberg) ConfirmOrdinaryRecovery(ctx context.Context, streams []types.StreamInterface, proofs []types.OrdinaryRecoveryTableProof, target string) ([]types.OrdinaryRecoveryTableProof, error) {
	i.resyncMu.Lock()
	defer i.resyncMu.Unlock()
	options, execution, err := ordinaryRecoveryContext(ctx)
	if err != nil {
		return nil, err
	}
	targetValue, err := ordinaryRecoveryLSN(target)
	if err != nil {
		return nil, err
	}
	if len(streams) == 0 || len(streams) != len(i.ordinaryRecovery) || len(proofs) != len(streams) || options.Plan == nil || options.Plan.TargetLSN != target {
		return nil, errors.New("ordinary recovery confirmation requires every selected table and the exact replay target")
	}
	provided := make(map[string]types.OrdinaryRecoveryTableProof, len(proofs))
	for _, proof := range proofs {
		if _, duplicate := provided[proof.Stream]; duplicate {
			return nil, errors.New("duplicate ordinary recovery table proof")
		}
		provided[proof.Stream] = proof
	}
	confirmed := make([]types.OrdinaryRecoveryTableProof, 0, len(streams))
	seen := make(map[string]struct{}, len(streams))
	for _, stream := range streams {
		if stream == nil {
			return nil, errors.New("nil ordinary recovery stream")
		}
		if _, duplicate := seen[stream.ID()]; duplicate {
			return nil, errors.New("duplicate ordinary recovery stream")
		}
		seen[stream.ID()] = struct{}{}
		r := i.ordinaryRecovery[stream.ID()]
		if r == nil || !r.prepared || r.proof.FenceSnapshotID == 0 || provided[stream.ID()] != r.proof || r.marker.Origin != options.Origin || r.marker.SourceIdentity != options.SourceIdentity || r.marker.ExecutionID != execution.ID || r.marker.LowerLSN != options.Plan.LowerLSN || targetValue != r.target {
			return nil, errors.New("ordinary recovery proof or execution binding changed")
		}
		ident := catalog.ToIdentifier(stream.GetDestinationDatabase(&i.config.IcebergDatabase), stream.GetDestinationTable())
		state := nativeTableEntry(i.catalog, ident)
		state.mu.Lock()
		tbl, loadErr := i.catalog.LoadTable(context.WithValue(ctx, nativeTableLockKey{}, state), ident)
		if loadErr == nil {
			loadErr = state.usable()
		}
		if loadErr == nil && state.sessions != 0 {
			loadErr = errors.New("ordinary recovery must close every writer before final confirmation")
		}
		if loadErr == nil {
			loadErr = r.validate(tbl)
		}
		position := ""
		if loadErr == nil {
			position, loadErr = ordinaryRecoveryPosition(tbl, targetValue)
		}
		state.mu.Unlock()
		if loadErr != nil {
			return nil, fmt.Errorf("ordinary recovery final table %q: %w", stream.ID(), loadErr)
		}
		value, err := ordinaryRecoveryLSN(position)
		if err != nil || value != targetValue {
			return nil, errors.New("ordinary recovery tables do not prove the exact common target")
		}
		proof := r.proof
		proof.CommittedLSN = position
		confirmed = append(confirmed, proof)
	}
	return confirmed, nil
}

func validateOrdinaryRecoveryWriter(ctx context.Context, state *nativeTableState, tbl *table.Table, stream types.StreamInterface) error {
	options, recovery := types.OrdinaryRecoveryFromContext(ctx)
	if !recovery {
		return nil
	}
	r := state.ordinaryRecovery
	execution, controlled := utils.ExecutionFromContext(ctx)
	target := uint64(0)
	var targetErr error
	if options.Plan != nil {
		target, targetErr = ordinaryRecoveryLSN(options.Plan.TargetLSN)
	}
	if r == nil || !r.prepared || !controlled || execution.ID != r.marker.ExecutionID || options.Plan == nil || targetErr != nil || target != r.target || r.proof.FenceSnapshotID == 0 || r.marker.Origin != options.Origin || r.marker.SourceIdentity != options.SourceIdentity || options.Plan.ReplayAfter[stream.ID()] != r.proof.CommittedLSN || options.Plan.LowerLSN != r.marker.LowerLSN {
		return errors.New("ordinary recovery writer does not match its prepared barrier and replay plan")
	}
	return r.validate(tbl)
}
