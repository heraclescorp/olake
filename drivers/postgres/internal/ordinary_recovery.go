package driver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"strconv"
	"strings"
	"time"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/drivers/postgres/internal/waljs"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/jackc/pglogrepl"
)

const ordinaryRecoverySlotQuery = `SELECT database, current_database() AS connected_database,
	plugin, slot_type, active, COALESCE(active_pid, 0) AS active_pid,
	COALESCE(confirmed_flush_lsn::text, '') AS confirmed_lsn,
	COALESCE(restart_lsn::text, '') AS restart_lsn,
	COALESCE(to_jsonb(s)->>'wal_status', '') AS wal_status,
	COALESCE(to_jsonb(s)->>'invalidation_reason', '') AS invalidation_reason,
	CASE WHEN pg_is_in_recovery() THEN pg_last_wal_replay_lsn() ELSE pg_current_wal_lsn() END::text AS current_lsn
	FROM pg_replication_slots s WHERE slot_name = $1`

type ordinaryRecoverySlot struct {
	Database           string `db:"database"`
	ConnectedDatabase  string `db:"connected_database"`
	Plugin             string `db:"plugin"`
	SlotType           string `db:"slot_type"`
	Active             bool   `db:"active"`
	ActivePID          int64  `db:"active_pid"`
	Confirmed          string `db:"confirmed_lsn"`
	Restart            string `db:"restart_lsn"`
	WALStatus          string `db:"wal_status"`
	InvalidationReason string `db:"invalidation_reason"`
	Current            string `db:"current_lsn"`
}

type ordinaryRecovery struct {
	identity   string
	lower      pglogrepl.LSN
	checkpoint pglogrepl.LSN
	target     pglogrepl.LSN
	start      pglogrepl.LSN
	plan       *types.OrdinaryRecoveryPlan
	complete   bool
}

func ordinaryRecoveryLowerLSN(raw string) (pglogrepl.LSN, error) {
	high, low, ok := strings.Cut(raw, "/")
	if !ok || high == "" || low == "" {
		return 0, fmt.Errorf("invalid PostgreSQL lower LSN %q", raw)
	}
	hi, err := strconv.ParseUint(high, 16, 32)
	if err != nil {
		return 0, err
	}
	lo, err := strconv.ParseUint(low, 16, 32)
	if err != nil {
		return 0, err
	}
	return pglogrepl.LSN(hi<<32 | lo), nil
}

func (p *Postgres) validateOrdinaryRecoveryCatalog(streams []types.StreamInterface, lower string) error {
	return p.validateRecoveryCatalog(streams, lower, false)
}

func (p *Postgres) validateRecoveryCatalog(streams []types.StreamInterface, lower string, incomplete bool) error {
	return p.validateRecoveryCatalogCheckpoint(streams, lower, lower, incomplete)
}

func (p *Postgres) validateRecoveryCatalogCheckpoint(streams []types.StreamInterface, lower, checkpoint string, incomplete bool) error {
	if !p.CDCSupport || p.config == nil || p.cdcConfig.ReplicationSlot == "" {
		return fmt.Errorf("ordinary recovery requires configured PostgreSQL CDC and an existing replication slot")
	}
	if p.resync != nil || len(streams) == 0 || p.state == nil || p.state.Type != types.GlobalType {
		return fmt.Errorf("ordinary recovery requires a nonempty frozen CDC catalog and saved GLOBAL state, not resync")
	}
	baseline, err := ordinaryRecoveryLowerLSN(lower)
	if err != nil || baseline == 0 {
		return fmt.Errorf("ordinary recovery requires a valid nonzero lower LSN %q", lower)
	}
	global := p.state.GetGlobal()
	if global == nil || global.State == nil || global.Streams == nil {
		return fmt.Errorf("ordinary recovery requires saved GLOBAL backfill membership for every selected stream")
	}
	var saved waljs.WALState
	if err := utils.Unmarshal(global.State, &saved); err != nil {
		return fmt.Errorf("invalid ordinary recovery GLOBAL state: %w", err)
	}
	savedLSN, err := ordinaryRecoveryLowerLSN(saved.LSN)
	checkpointLSN, checkpointErr := ordinaryRecoveryLowerLSN(checkpoint)
	if err != nil || checkpointErr != nil || checkpointLSN < baseline || (savedLSN != baseline && savedLSN != checkpointLSN) {
		return fmt.Errorf("ordinary recovery saved GLOBAL checkpoint does not match the immutable baseline or verified checkpoint")
	}
	seen := make(map[string]bool, len(streams))
	for _, stream := range streams {
		if stream == nil || seen[stream.ID()] {
			return fmt.Errorf("ordinary recovery catalog contains a missing or duplicate stream")
		}
		seen[stream.ID()] = true
		if (stream.GetSyncMode() != types.CDC && stream.GetSyncMode() != types.STRICTCDC) || (!incomplete && !global.Streams.Exists(stream.ID())) || (incomplete && stream.GetSyncMode() != types.CDC) {
			return fmt.Errorf("ordinary recovery stream %s requires CDC and saved backfill membership unless recovering a validated backfill candidate", stream.ID())
		}
		if keys := stream.GetStream().SourceDefinedPrimaryKey; keys == nil || keys.Len() == 0 {
			return fmt.Errorf("ordinary recovery requires existing primary keys for stream %s", stream.ID())
		}
	}
	return nil
}

func (p *Postgres) ordinaryRecoverySlot(ctx context.Context, lower pglogrepl.LSN, ownedPID int64) (waljs.ReplicationSlot, error) {
	var raw ordinaryRecoverySlot
	if err := p.client.GetContext(ctx, &raw, ordinaryRecoverySlotQuery, p.cdcConfig.ReplicationSlot); err != nil {
		return waljs.ReplicationSlot{}, fmt.Errorf("read ordinary recovery replication slot: %w", err)
	}
	return validateOrdinaryRecoverySlot(raw, p.config.Database, p.cdcConfig.Publication, lower, ownedPID)
}

func validateOrdinaryRecoverySlot(raw ordinaryRecoverySlot, database, publication string, lower pglogrepl.LSN, ownedPID int64) (waljs.ReplicationSlot, error) {
	if raw.Database != database || raw.ConnectedDatabase != database || raw.SlotType != "logical" {
		return waljs.ReplicationSlot{}, fmt.Errorf("ordinary recovery slot must be logical in configured database %q", database)
	}
	if (ownedPID == 0 && raw.Active) || (ownedPID != 0 && (!raw.Active || raw.ActivePID != ownedPID)) {
		return waljs.ReplicationSlot{}, fmt.Errorf("ordinary recovery requires an inactive slot or its own WAL sender")
	}
	if raw.Plugin != "pgoutput" && raw.Plugin != "wal2json" {
		return waljs.ReplicationSlot{}, fmt.Errorf("ordinary recovery does not support output plugin %q", raw.Plugin)
	}
	if raw.Plugin == "pgoutput" && publication == "" {
		return waljs.ReplicationSlot{}, fmt.Errorf("ordinary recovery pgoutput slot requires the configured publication")
	}
	if raw.InvalidationReason != "" || (raw.WALStatus != "" && raw.WALStatus != "reserved" && raw.WALStatus != "extended") {
		return waljs.ReplicationSlot{}, fmt.Errorf("ordinary recovery slot WAL is unavailable (%s, %s)", raw.WALStatus, raw.InvalidationReason)
	}
	confirmed, err := pglogrepl.ParseLSN(raw.Confirmed)
	if err != nil || confirmed < lower {
		return waljs.ReplicationSlot{}, fmt.Errorf("ordinary recovery slot %q is below saved baseline %s", raw.Confirmed, lower)
	}
	restart, err := pglogrepl.ParseLSN(raw.Restart)
	if err != nil || restart == 0 || restart > confirmed {
		return waljs.ReplicationSlot{}, fmt.Errorf("ordinary recovery slot has no usable retained WAL restart position")
	}
	current, err := pglogrepl.ParseLSN(raw.Current)
	if err != nil || current < confirmed {
		return waljs.ReplicationSlot{}, fmt.Errorf("ordinary recovery slot is ahead of available source WAL")
	}
	return waljs.ReplicationSlot{Plugin: raw.Plugin, SlotType: raw.SlotType, LSN: confirmed, CurrentLSN: current}, nil
}

func validateBackfillRecoverySlot(raw ordinaryRecoverySlot, lower pglogrepl.LSN) error {
	confirmed, err := pglogrepl.ParseLSN(raw.Confirmed)
	if err != nil || confirmed != lower {
		return fmt.Errorf("backfill recovery requires an unadvanced slot at the original lower LSN")
	}
	restart, err := pglogrepl.ParseLSN(raw.Restart)
	if err != nil || restart == 0 || restart > lower {
		return fmt.Errorf("backfill recovery requires retained WAL at the original lower LSN")
	}
	return nil
}

func (p *Postgres) identifyOrdinaryRecovery(ctx context.Context, slot waljs.ReplicationSlot) (string, error) {
	config, err := p.prepareWALJSConfig()
	if err != nil {
		return "", err
	}
	conn, err := waljs.ConnectReplication(ctx, config)
	if err != nil {
		return "", err
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = conn.Close(cleanupCtx)
	}()
	system, err := pglogrepl.IdentifySystem(ctx, conn)
	if err != nil {
		return "", fmt.Errorf("identify ordinary recovery source: %w", err)
	}
	if system.DBName != p.config.Database {
		return "", fmt.Errorf("ordinary recovery replication connection database differs from configured database")
	}
	return waljs.SourceIdentity(system, p.cdcConfig.ReplicationSlot, slot.Plugin)
}

// ValidateOrdinaryRecovery runs before any destination barrier. Historical server
// identity was not saved by ordinary sync: only exact original config artifacts
// and this independently checked live identity are claimed by recovery.
func (p *Postgres) ValidateOrdinaryRecovery(ctx context.Context, streams []types.StreamInterface, lower string) (string, error) {
	if err := utils.CheckExecutionFence(ctx); err != nil {
		return "", err
	}
	if _, resync := types.ResyncFromContext(ctx); resync {
		return "", fmt.Errorf("ordinary recovery cannot use resync context")
	}
	options, recovery := types.OrdinaryRecoveryFromContext(ctx)
	incomplete := false
	if recovery && options.Origin.Backfill && (options.BackfillCheckpoint == nil || options.BackfillState == nil) {
		return "", fmt.Errorf("backfill recovery requires a validated candidate and immutable baseline")
	}
	if recovery && options.Origin.Backfill && (!options.BackfillComplete || options.CheckpointLSN != "") {
		for _, stream := range streams {
			if stream != nil && (p.state == nil || !p.state.HasCompletedBackfill(stream.Self())) {
				incomplete = true
			}
		}
	}
	checkpoint, err := p.ordinaryRecoveryCheckpointLSN(options, lower)
	if err != nil {
		return "", err
	}
	if err := p.validateRecoveryCatalogCheckpoint(streams, lower, checkpoint.String(), incomplete); err != nil {
		return "", err
	}
	if err := p.cdcConfig.validateToastRecovery(); err != nil {
		return "", err
	}
	if !p.cdcConfig.sourceToastRecovery() {
		if err := validateToastStreams(streams); err != nil {
			return "", err
		}
	}
	if p.cdcConfig.sourceToastRecovery() {
		var standby bool
		if err := p.client.GetContext(ctx, &standby, "SELECT pg_is_in_recovery()"); err != nil {
			return "", fmt.Errorf("validate ordinary recovery source TOAST host: %w", err)
		}
		if standby {
			return "", fmt.Errorf("source TOAST recovery requires a primary CDC host")
		}
	}
	if err := validatePublicationContainsStreams(ctx, p.client, p.cdcConfig.Publication, streams); err != nil {
		return "", err
	}
	baseline, _ := pglogrepl.ParseLSN(lower)
	slot, err := p.ordinaryRecoverySlot(ctx, baseline, 0)
	if err != nil {
		return "", err
	}
	if checkpoint > slot.CurrentLSN {
		return "", fmt.Errorf("ordinary recovery verified checkpoint is beyond available source WAL")
	}
	if incomplete && slot.LSN == baseline {
		var raw ordinaryRecoverySlot
		if err := p.client.GetContext(ctx, &raw, ordinaryRecoverySlotQuery, p.cdcConfig.ReplicationSlot); err != nil {
			return "", err
		}
		if err := validateBackfillRecoverySlot(raw, baseline); err != nil {
			return "", err
		}
	}
	identity, err := p.identifyOrdinaryRecovery(ctx, slot)
	if err != nil {
		return "", err
	}
	if recovery && options.Origin.Backfill && (options.BackfillCheckpoint.ServerIdentity != identity || options.BackfillCheckpoint.SlotName != p.cdcConfig.ReplicationSlot) {
		return "", fmt.Errorf("backfill recovery source identity or slot differs from the candidate")
	}
	if options, ok := types.OrdinaryRecoveryFromContext(ctx); ok && options.SourceIdentity != "" && options.SourceIdentity != identity {
		return "", fmt.Errorf("ordinary recovery source identity changed after validation")
	}
	if p.ordinary != nil && (p.ordinary.identity != identity || p.ordinary.lower != baseline) {
		return "", fmt.Errorf("ordinary recovery source identity or baseline changed")
	}
	if p.ordinary == nil {
		p.ordinary = &ordinaryRecovery{identity: identity, lower: baseline}
	}
	if incomplete && slot.LSN > baseline {
		options.BackfillAcknowledgedLSN = slot.LSN.String()
	}
	return identity, nil
}

func (p *Postgres) ordinaryRecoveryCheckpointLSN(options *types.OrdinaryRecoveryOptions, lower string) (pglogrepl.LSN, error) {
	baseline, err := ordinaryRecoveryLowerLSN(lower)
	if err != nil || baseline == 0 {
		return 0, fmt.Errorf("ordinary recovery requires a valid nonzero lower LSN")
	}
	checkpoint := baseline
	if p.ordinary != nil {
		checkpoint = max(checkpoint, p.ordinary.checkpoint)
	}
	if options != nil && options.CheckpointLSN != "" {
		proven, err := ordinaryRecoveryLowerLSN(options.CheckpointLSN)
		if err != nil || proven < checkpoint {
			return 0, fmt.Errorf("ordinary recovery verified checkpoint is invalid or regresses completed recovery")
		}
		checkpoint = proven
	}
	if options != nil && options.Plan != nil && options.Plan.TargetLSN != "" {
		target, err := ordinaryRecoveryLowerLSN(options.Plan.TargetLSN)
		if err != nil || target < checkpoint {
			return 0, fmt.Errorf("ordinary recovery fixed target is below the verified checkpoint")
		}
	}
	return checkpoint, nil
}

// ordinaryRecoveryPlan uses all table positions, never map iteration order or a
// group-wide 'already finished' shortcut. Missing metadata is not baseline proof.
func ordinaryRecoveryPlan(streams []types.StreamInterface, lower string, positions map[string]string, slot pglogrepl.LSN) (*types.OrdinaryRecoveryPlan, error) {
	baseline, err := ordinaryRecoveryLowerLSN(lower)
	if err != nil || baseline == 0 || len(streams) == 0 || len(positions) != len(streams) {
		return nil, fmt.Errorf("ordinary recovery requires the baseline and exact selected table positions")
	}
	plan := &types.OrdinaryRecoveryPlan{LowerLSN: baseline.String(), ReplayAfter: make(map[string]string, len(streams))}
	target := baseline
	for _, stream := range streams {
		position, err := pglogrepl.ParseLSN(positions[stream.ID()])
		if err != nil || position < baseline {
			return nil, fmt.Errorf("ordinary recovery table %s has missing, invalid, or below-baseline metadata", stream.ID())
		}
		if _, duplicate := plan.ReplayAfter[stream.ID()]; duplicate {
			return nil, fmt.Errorf("ordinary recovery has duplicate stream %s", stream.ID())
		}
		plan.ReplayAfter[stream.ID()] = position.String()
		target = max(target, position)
	}
	if slot < baseline || slot > target {
		return nil, fmt.Errorf("ordinary recovery slot %s is outside baseline %s and authoritative target %s", slot, baseline, target)
	}
	for stream, raw := range plan.ReplayAfter {
		position, _ := pglogrepl.ParseLSN(raw)
		if position < slot {
			return nil, fmt.Errorf("ordinary recovery table %s does not cover already-acknowledged slot %s", stream, slot)
		}
	}
	plan.TargetLSN = target.String()
	return plan, nil
}

func ordinaryRecoveryPlanAtCheckpoint(streams []types.StreamInterface, lower string, positions map[string]string, slot, checkpoint, fixedTarget pglogrepl.LSN) (*types.OrdinaryRecoveryPlan, error) {
	plan, err := ordinaryRecoveryPlan(streams, lower, positions, slot)
	if err != nil {
		return nil, err
	}
	baseline, _ := ordinaryRecoveryLowerLSN(plan.LowerLSN)
	target, _ := ordinaryRecoveryLowerLSN(plan.TargetLSN)
	if checkpoint < baseline || (fixedTarget != 0 && fixedTarget < target) {
		return nil, fmt.Errorf("ordinary recovery checkpoint or table metadata is outside the immutable recovery range")
	}
	if fixedTarget != 0 {
		target = fixedTarget
	}
	if checkpoint > target || slot > target {
		return nil, fmt.Errorf("ordinary recovery checkpoint or slot exceeds the fixed target")
	}
	for stream, raw := range plan.ReplayAfter {
		position, _ := ordinaryRecoveryLowerLSN(raw)
		if position < checkpoint {
			return nil, fmt.Errorf("ordinary recovery table %s does not cover verified checkpoint %s", stream, checkpoint)
		}
	}
	plan.TargetLSN = target.String()
	return plan, nil
}

func (p *Postgres) PrepareOrdinaryRecovery(ctx context.Context, streams []types.StreamInterface, lower string, positions map[string]string) (*types.OrdinaryRecoveryPlan, error) {
	options, ok := types.OrdinaryRecoveryFromContext(ctx)
	if !ok || options.SourceIdentity == "" {
		return nil, fmt.Errorf("ordinary recovery requires the pre-barrier source identity")
	}
	if options.Origin.Backfill && !options.BackfillComplete {
		return nil, fmt.Errorf("backfill recovery must finish all tables before preparing CDC")
	}
	if p.ordinary == nil || p.ordinary.plan != nil || p.replicator != nil {
		return nil, fmt.Errorf("ordinary recovery source was not validated or is already prepared")
	}
	if options.CheckpointInterval < 0 {
		return nil, fmt.Errorf("ordinary recovery checkpoint interval must be positive")
	}
	if _, err := p.ValidateOrdinaryRecovery(ctx, streams, lower); err != nil {
		return nil, err
	}
	checkpoint, err := p.ordinaryRecoveryCheckpointLSN(options, lower)
	if err != nil {
		return nil, err
	}
	var saved waljs.WALState
	if err := utils.Unmarshal(p.state.GetGlobal().State, &saved); err != nil {
		return nil, err
	}
	savedLSN, err := ordinaryRecoveryLowerLSN(saved.LSN)
	if err != nil || savedLSN != checkpoint {
		return nil, fmt.Errorf("ordinary recovery requires the verified checkpoint state before preparing replay")
	}
	fixedTarget := p.ordinary.target
	if options.Plan != nil && options.Plan.TargetLSN != "" {
		restored, err := ordinaryRecoveryLowerLSN(options.Plan.TargetLSN)
		if err != nil || restored == 0 || (fixedTarget != 0 && fixedTarget != restored) {
			return nil, fmt.Errorf("ordinary recovery fixed target changed between batches")
		}
		fixedTarget = restored
	}
	baseline, _ := ordinaryRecoveryLowerLSN(lower)
	slot, err := p.ordinaryRecoverySlot(ctx, baseline, 0)
	if err != nil {
		return nil, err
	}
	if options.Origin.Backfill {
		boundary, err := backfillRecoveryTarget(options.BackfillTargetLSN, max(checkpoint, fixedTarget), slot.CurrentLSN)
		if err != nil || (fixedTarget != 0 && boundary != fixedTarget) {
			return nil, fmt.Errorf("ordinary recovery fixed backfill target is invalid or changed")
		}
		fixedTarget = boundary
	}
	plan, err := ordinaryRecoveryPlanAtCheckpoint(streams, lower, positions, slot.LSN, checkpoint, fixedTarget)
	if err != nil {
		return nil, err
	}
	target, _ := ordinaryRecoveryLowerLSN(plan.TargetLSN)
	if target > slot.CurrentLSN {
		return nil, fmt.Errorf("ordinary recovery authoritative target is beyond available source WAL")
	}
	p.streams = append([]types.StreamInterface(nil), streams...)
	p.ordinary.checkpoint = checkpoint
	p.ordinary.start = max(checkpoint, slot.LSN)
	p.ordinary.target = target
	p.ordinary.complete = false
	p.ordinary.plan = &types.OrdinaryRecoveryPlan{LowerLSN: plan.LowerLSN, TargetLSN: plan.TargetLSN, ReplayAfter: maps.Clone(plan.ReplayAfter)}
	return plan, nil
}

func (p *Postgres) requireOrdinaryRecoveryPlan(ctx context.Context, plan *types.OrdinaryRecoveryPlan) error {
	options, ok := types.OrdinaryRecoveryFromContext(ctx)
	if !ok || p.ordinary == nil || p.ordinary.plan == nil || plan == nil || options.SourceIdentity != p.ordinary.identity ||
		plan.LowerLSN != p.ordinary.plan.LowerLSN || plan.TargetLSN != p.ordinary.plan.TargetLSN || plan.BatchTargetLSN != p.ordinary.plan.BatchTargetLSN || !maps.Equal(plan.ReplayAfter, p.ordinary.plan.ReplayAfter) {
		return fmt.Errorf("ordinary recovery does not match the validated source and prepared plan")
	}
	return nil
}

func (p *Postgres) streamOrdinaryRecoveryChanges(ctx context.Context, metadata map[string]any, callback abstract.CDCMsgFn) (any, error) {
	options, ok := types.OrdinaryRecoveryFromContext(ctx)
	if !ok {
		return nil, fmt.Errorf("ordinary recovery requires its bounded replay context")
	}
	if err := p.requireOrdinaryRecoveryPlan(ctx, options.Plan); err != nil {
		return nil, err
	}
	if p.ordinary.complete || p.replicator != nil {
		return nil, fmt.Errorf("ordinary recovery batch was already read")
	}
	plan := p.ordinary.plan
	if len(metadata) != len(plan.ReplayAfter) {
		return nil, fmt.Errorf("ordinary recovery writer membership differs from the frozen plan")
	}
	for stream, expected := range plan.ReplayAfter {
		raw, ok := metadata[stream].(string)
		var state waljs.WALState
		if !ok || json.Unmarshal([]byte(raw), &state) != nil {
			return nil, fmt.Errorf("ordinary recovery writer metadata missing for stream %s", stream)
		}
		position, err := pglogrepl.ParseLSN(state.LSN)
		if err != nil || position.String() != expected {
			return nil, fmt.Errorf("ordinary recovery writer metadata changed after barrier for stream %s", stream)
		}
	}
	baseline, _ := pglogrepl.ParseLSN(plan.LowerLSN)
	slot, err := p.ordinaryRecoverySlot(ctx, baseline, 0)
	if err != nil {
		return nil, err
	}
	target, _ := ordinaryRecoveryLowerLSN(plan.TargetLSN)
	if _, err := ordinaryRecoveryPlanAtCheckpoint(p.streams, plan.LowerLSN, plan.ReplayAfter, slot.LSN, p.ordinary.checkpoint, target); err != nil {
		return nil, err
	}
	batch := target
	var sourceTime time.Time
	if slot.LSN != target {
		config, err := p.prepareWALJSConfig(p.streams...)
		if err != nil {
			return nil, err
		}
		config.ReplayAfter = make(map[string]pglogrepl.LSN, len(plan.ReplayAfter))
		allCovered := true
		for stream, raw := range plan.ReplayAfter {
			config.ReplayAfter[stream], _ = pglogrepl.ParseLSN(raw)
			allCovered = allCovered && config.ReplayAfter[stream] == target
		}
		config.StrictRecovery = true
		config.ExpectedSourceIdentity = p.ordinary.identity
		config.RecoveryStartLSN = max(p.ordinary.checkpoint, slot.LSN)
		config.RecoveryCheckpointInterval = options.CheckpointInterval
		p.replicator, err = waljs.NewReplicator(ctx, config, slot, &target, p.dataTypeConverter, p.columnType)
		if err != nil {
			return nil, err
		}
		if allCovered {
			// Every table independently proves the complete target already. Start
			// the owned sender without decoding a new event; its exact ACK remains
			// deferred until protocol confirms the post-barrier common boundary.
			p.replicator.Socket().ClientXLogPos = target
		}
		// Preserve configured TOAST semantics: source mode intentionally hydrates
		// current source values. The protocol reports this event-time caveat.
		if err := p.streamChangesWithToast(ctx, callback); err != nil {
			return nil, err
		}
		socket := p.replicator.Socket()
		batch = socket.RecoveryBatchLSN
		if socket.TransactionOpen || batch == 0 || socket.ClientXLogPos != batch || batch < p.ordinary.start || batch > target || (batch == p.ordinary.start && batch != target) {
			return nil, fmt.Errorf("ordinary recovery did not finish at a prepared complete-transaction batch before target %s", target)
		}
		sourceTime = socket.RecoverySourceTime
	} else {
		identity, err := p.identifyOrdinaryRecovery(ctx, slot)
		if err != nil {
			return nil, err
		}
		if identity != p.ordinary.identity {
			return nil, fmt.Errorf("ordinary recovery source identity changed before no-replay completion")
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.ordinary.complete = true
	plan.BatchTargetLSN = batch.String()
	plan.BatchSourceTime = sourceTime
	options.Plan.BatchTargetLSN = plan.BatchTargetLSN
	options.Plan.BatchSourceTime = sourceTime
	logger.Infof("postgres_ordinary_recovery_batch_completed batch_lsn=%s final_target_lsn=%s", batch, target)
	// Every selected writer receives this metadata, including tables with no rows.
	return waljs.WALState{LSN: plan.BatchTargetLSN}, nil
}

func (p *Postgres) AcknowledgeOrdinaryRecovery(ctx context.Context, plan *types.OrdinaryRecoveryPlan) error {
	if err := utils.CheckExecutionFence(ctx); err != nil {
		return err
	}
	if err := p.requireOrdinaryRecoveryPlan(ctx, plan); err != nil {
		return err
	}
	if !p.ordinary.complete || plan.BatchTargetLSN == "" {
		return fmt.Errorf("ordinary recovery cannot acknowledge incomplete bounded replay")
	}
	target, err := ordinaryRecoveryLowerLSN(plan.BatchTargetLSN)
	final, finalErr := ordinaryRecoveryLowerLSN(plan.TargetLSN)
	if err != nil || finalErr != nil || target < p.ordinary.start || target > final {
		return fmt.Errorf("ordinary recovery acknowledgement requires its explicit prepared batch boundary")
	}
	baseline, _ := pglogrepl.ParseLSN(plan.LowerLSN)
	var ownedPID int64
	if p.replicator != nil {
		socket := p.replicator.Socket()
		if socket.TransactionOpen || socket.ClientXLogPos != target || socket.RecoveryBatchLSN != target || socket.CurrentWalPosition != final {
			return fmt.Errorf("ordinary recovery acknowledgement requires the exact completed batch and immutable final target")
		}
		ownedPID = int64(socket.BackendPID())
	}
	slot, err := p.ordinaryRecoverySlot(ctx, baseline, ownedPID)
	if err != nil {
		return err
	}
	if slot.LSN > target {
		return fmt.Errorf("ordinary recovery slot is ahead of the confirmed target")
	}
	if p.replicator == nil {
		identity, err := p.identifyOrdinaryRecovery(ctx, slot)
		if err != nil {
			return err
		}
		if identity != p.ordinary.identity {
			return fmt.Errorf("ordinary recovery source identity changed before acknowledgement")
		}
	}
	if slot.LSN != target {
		if p.replicator == nil {
			return fmt.Errorf("ordinary recovery has no WAL sender for acknowledgement")
		}
		if err := waljs.AcknowledgeLSN(ctx, p.client, p.replicator.Socket(), false); err != nil {
			return err
		}
	}
	// ACK-before-state is deliberate. An interrupted retry re-derives coverage
	// from all durable tables and permits slot > baseline only if all cover it.
	if err := utils.CheckExecutionFence(ctx); err != nil {
		return err
	}
	global, err := ordinaryRecoveryCheckpoint(p.state.GetGlobal().State, plan.BatchTargetLSN)
	if err != nil {
		return err
	}
	p.state.SetGlobal(global)
	p.ordinary.checkpoint = target
	logger.Infof("postgres_ordinary_recovery_batch_acknowledged batch_lsn=%s final_target_lsn=%s", target, final)
	return nil
}

func (p *Postgres) CleanupOrdinaryRecovery(ctx context.Context) error {
	var err error
	if p.replicator != nil {
		err = waljs.Cleanup(ctx, p.replicator.Socket())
		p.replicator = nil
	}
	if p.ordinary != nil {
		p.ordinary.plan = nil
		p.ordinary.complete = false
		p.ordinary.start = 0
	}
	return err
}

var _ abstract.OrdinaryRecoverySource = (*Postgres)(nil)

func ordinaryRecoveryCheckpoint(raw any, target string) (map[string]any, error) {
	var checkpoint map[string]any
	if saved, ok := raw.(map[string]any); ok {
		checkpoint = maps.Clone(saved)
	} else {
		encoded, err := json.Marshal(raw)
		if err != nil {
			return nil, fmt.Errorf("ordinary recovery cannot preserve GLOBAL checkpoint: %w", err)
		}
		decoder := json.NewDecoder(bytes.NewReader(encoded))
		decoder.UseNumber()
		if err := decoder.Decode(&checkpoint); err != nil {
			return nil, fmt.Errorf("ordinary recovery cannot preserve GLOBAL checkpoint: %w", err)
		}
	}
	if checkpoint == nil {
		return nil, fmt.Errorf("ordinary recovery cannot preserve an empty GLOBAL checkpoint")
	}
	checkpoint["lsn"] = target
	return checkpoint, nil
}

func (p *Postgres) CaptureBackfillRecoveryTarget(ctx context.Context, lower string) (target string, err error) {
	options, ok := types.OrdinaryRecoveryFromContext(ctx)
	if !ok || !options.Origin.Backfill || !options.BackfillComplete || p.ordinary == nil || p.ordinary.identity != options.SourceIdentity {
		return "", fmt.Errorf("backfill target capture requires completed backfill and validated source identity")
	}
	if err := utils.CheckExecutionFence(ctx); err != nil {
		return "", err
	}
	baseline, err := ordinaryRecoveryLowerLSN(lower)
	if err != nil || baseline == 0 || baseline != p.ordinary.lower {
		return "", fmt.Errorf("backfill target capture requires the original lower LSN")
	}
	slot, err := p.ordinaryRecoverySlot(ctx, baseline, 0)
	if err != nil {
		return "", err
	}
	config, err := p.prepareWALJSConfig()
	if err != nil {
		return "", err
	}
	conn, err := waljs.ConnectReplication(ctx, config)
	if err != nil {
		return "", err
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if closeErr := conn.Close(cleanupCtx); err == nil && closeErr != nil {
			err = fmt.Errorf("close temporary backfill boundary slot: %w", closeErr)
		}
	}()
	system, err := pglogrepl.IdentifySystem(ctx, conn)
	if err != nil {
		return "", err
	}
	identity, err := waljs.SourceIdentity(system, p.cdcConfig.ReplicationSlot, slot.Plugin)
	if err != nil || system.DBName != p.config.Database || identity != options.SourceIdentity {
		return "", fmt.Errorf("backfill target source identity or database changed")
	}
	if err := utils.CheckExecutionFence(ctx); err != nil {
		return "", err
	}
	created, err := pglogrepl.CreateReplicationSlot(ctx, conn, "olake_backfill_"+strings.ToLower(utils.ULID()), slot.Plugin, pglogrepl.CreateReplicationSlotOptions{
		Temporary: true, Mode: pglogrepl.LogicalReplication, SnapshotAction: "NOEXPORT_SNAPSHOT",
	})
	if err != nil {
		return "", fmt.Errorf("capture backfill transaction boundary; one spare logical replication slot is required: %w", err)
	}
	boundary, err := pglogrepl.ParseLSN(created.ConsistentPoint)
	if err != nil || boundary == 0 || boundary < baseline || boundary < slot.LSN {
		return "", fmt.Errorf("temporary backfill slot returned an invalid transaction boundary")
	}
	if err := utils.CheckExecutionFence(ctx); err != nil {
		return "", err
	}
	return boundary.String(), nil
}

func backfillRecoveryTarget(raw string, minimum, current pglogrepl.LSN) (pglogrepl.LSN, error) {
	boundary, err := pglogrepl.ParseLSN(raw)
	if err != nil || boundary == 0 || boundary < minimum || boundary > current {
		return 0, fmt.Errorf("backfill recovery requires its exact captured transaction boundary covering every table within available WAL")
	}
	return boundary, nil
}
