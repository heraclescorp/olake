package driver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/drivers/postgres/internal/waljs"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	s3util "github.com/datazip-inc/olake/utils/s3"
	"github.com/jackc/pglogrepl"
	"github.com/spf13/viper"
)

func hasCopyOnWrite(streams []types.StreamInterface) bool {
	for _, stream := range streams {
		if stream.Self().CopyOnWriteEnabled() {
			return true
		}
	}
	return false
}

func (p *Postgres) ValidateCopyOnWrite(ctx context.Context, streams []types.StreamInterface) error {
	if !hasCopyOnWrite(streams) {
		return nil
	}
	if !p.CDCSupport || p.config == nil || p.cdcConfig.ReplicationSlot == "" {
		return fmt.Errorf("copy-on-write requires PostgreSQL CDC with an existing pgoutput slot")
	}
	if err := p.cdcConfig.validateToastRecovery(); err != nil {
		return err
	}
	if !p.cdcConfig.sourceToastRecovery() {
		return fmt.Errorf("copy-on-write requires source TOAST hydration; destination TOAST/index hydration is unsupported")
	}
	slot, err := waljs.GetSlotPosition(ctx, p.client, p.cdcConfig.ReplicationSlot)
	if err != nil {
		return err
	}
	if slot.Plugin != "pgoutput" || slot.SlotType != "logical" || p.cdcConfig.Publication == "" {
		return fmt.Errorf("copy-on-write requires a logical pgoutput slot and configured publication")
	}
	for _, stream := range streams {
		if !stream.Self().CopyOnWriteEnabled() {
			continue
		}
		if stream.GetSyncMode() != types.CDC && stream.GetSyncMode() != types.STRICTCDC {
			return fmt.Errorf("stream %s: copy-on-write requires CDC or strict_cdc", stream.ID())
		}
		live, err := p.ProduceSchema(ctx, types.StreamID{Namespace: stream.Namespace(), Name: stream.Name()})
		if err != nil {
			return fmt.Errorf("validate live copy-on-write keys for %s: %w", stream.ID(), err)
		}
		configured := stream.GetStream().SourceDefinedPrimaryKey
		if configured == nil || live.SourceDefinedPrimaryKey.Len() == 0 || live.SourceDefinedPrimaryKey.Len() != configured.Len() {
			return fmt.Errorf("stream %s: copy-on-write source primary keys changed or are unavailable; rediscover the catalog", stream.ID())
		}
		for _, key := range live.SourceDefinedPrimaryKey.Array() {
			if !configured.Exists(key) {
				return fmt.Errorf("stream %s: copy-on-write source primary keys changed; rediscover the catalog", stream.ID())
			}
		}
		// Validate actual current key types/nullability with the same contract as
		// destination setup, not a saved discovery capability or stale key schema.
		live.DefaultStreamProperties = stream.GetStream().DefaultStreamProperties
		current := *stream.Self()
		current.Stream = live
		if err := current.ValidateCopyOnWrite(types.Iceberg); err != nil {
			return fmt.Errorf("stream %s: live source schema: %w", stream.ID(), err)
		}
	}
	return nil
}

// AcknowledgeCDCCheckpoint is called only after all selected table commits have
// returned success. It preserves the live replication socket for the next batch.
func (p *Postgres) AcknowledgeCDCCheckpoint(ctx context.Context, raw any) (err error) {
	if _, recovery := types.OrdinaryRecoveryFromContext(ctx); recovery {
		return fmt.Errorf("ordinary recovery permits terminal proof-backed acknowledgement only")
	}
	if _, resync := types.ResyncFromContext(ctx); resync {
		return fmt.Errorf("atomic resync permits terminal publication-backed acknowledgement only")
	}
	if err := utils.CheckExecutionFence(ctx); err != nil {
		return err
	}
	if p.replicator == nil || !p.replicator.Socket().CommitAligned {
		return fmt.Errorf("copy-on-write checkpoint requires an active commit-aligned PostgreSQL sender")
	}
	var checkpoint waljs.WALState
	if err := utils.Unmarshal(raw, &checkpoint); err != nil {
		return err
	}
	boundary, err := pglogrepl.ParseLSN(checkpoint.LSN)
	if err != nil || boundary == 0 {
		return fmt.Errorf("invalid complete-COMMIT checkpoint %q", checkpoint.LSN)
	}
	socket := p.replicator.Socket()
	if socket.TransactionOpen || socket.ClientXLogPos != boundary || boundary < socket.ConfirmedFlushLSN {
		return fmt.Errorf("copy-on-write checkpoint is not the active sender's complete transaction boundary")
	}
	settled := utils.BeginExecutionParticipant(ctx)
	defer func() { settled(err == nil) }()
	if err := p.persistCopyOnWriteCheckpoint(ctx, checkpoint); err != nil {
		return err
	}
	if err := utils.CheckExecutionFence(ctx); err != nil {
		return err
	}
	if err := waljs.AcknowledgeLSN(ctx, p.client, socket, false); err != nil {
		return err
	}
	socket.ConfirmedFlushLSN = boundary
	return nil
}

func (p *Postgres) persistCopyOnWriteCheckpoint(ctx context.Context, checkpoint waljs.WALState) error {
	if p.state == nil || p.state.SuppressPersistence {
		return fmt.Errorf("copy-on-write checkpoint requires persistent GLOBAL state")
	}
	// Hold the state lock through publication: concurrent backfill/state logging
	// must not overwrite a candidate or race the complete GLOBAL membership copy.
	p.state.Lock()
	defer p.state.Unlock()
	if p.state.Type != types.GlobalType || p.state.Global == nil || p.state.Global.Streams == nil {
		return fmt.Errorf("copy-on-write checkpoint requires complete GLOBAL membership")
	}
	for _, stream := range p.streams {
		if !p.state.Global.Streams.Exists(stream.ID()) {
			return fmt.Errorf("copy-on-write checkpoint missing backfill membership for %s", stream.ID())
		}
	}
	sourceState, err := copyOnWriteSourceState(p.state.Global.State, checkpoint)
	if err != nil {
		return err
	}
	candidate := *p.state
	candidate.Global = &types.GlobalState{State: sourceState, Streams: p.state.Global.Streams}
	data, err := candidate.MarshalJSON()
	if err != nil {
		return err
	}
	stateFile := viper.GetString(constants.StatePath)
	if stateFile == "" {
		return fmt.Errorf("copy-on-write requires a durable state path")
	}
	if err := writeCopyOnWriteCheckpoint(stateFile, data); err != nil {
		return fmt.Errorf("persist complete copy-on-write checkpoint: %w", err)
	}
	// Every table already covers this state. Retain it on later remote/ACK
	// uncertainty so terminal cleanup cannot replace it with the old baseline.
	p.state.Global.State = sourceState
	if s3util.JobBucket != "" {
		key := path.Join(s3util.JobPrefix, "state.json")
		if err := utils.CheckExecutionFence(ctx); err != nil {
			return err
		}
		if err := s3util.UploadFileToS3(ctx, stateFile, s3util.JobBucket, key); err != nil {
			return fmt.Errorf("publish complete copy-on-write checkpoint: %w", err)
		}
		object, err := s3util.GetObject(ctx, s3util.JobBucket, key)
		if err != nil {
			return fmt.Errorf("verify copy-on-write checkpoint publication: %w", err)
		}
		defer object.Body.Close()
		published, err := io.ReadAll(io.LimitReader(object.Body, int64(len(data))+1))
		if err != nil {
			return fmt.Errorf("read authoritative copy-on-write checkpoint: %w", err)
		}
		if !bytes.Equal(published, data) {
			return fmt.Errorf("copy-on-write authoritative checkpoint bytes differ from the complete committed state")
		}
	}
	return utils.CheckExecutionFence(ctx)
}

func copyOnWriteSourceState(previous any, checkpoint waljs.WALState) (any, error) {
	if _, typed := previous.(waljs.WALState); typed {
		return checkpoint, nil
	}
	data, err := json.Marshal(previous)
	if err != nil {
		return nil, err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil || fields == nil {
		return nil, fmt.Errorf("copy-on-write saved source checkpoint must be an object")
	}
	lsn, err := json.Marshal(checkpoint.LSN)
	if err != nil {
		return nil, err
	}
	fields["lsn"] = lsn
	return fields, nil
}

// State may contain sensitive source values; private temporary files also avoid
// exposing a torn checkpoint before its atomic rename and directory fsync.
func writeCopyOnWriteCheckpoint(name string, data []byte) error {
	file, err := os.CreateTemp(filepath.Dir(name), ".cow-checkpoint-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	if _, err := file.Write(data); err != nil {
		file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(file.Name(), name); err != nil {
		return err
	}
	directory, err := os.Open(filepath.Dir(name))
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

// A failed multi-table batch can leave several independent high-watermarks.
// Reconcile each one, never choose an arbitrary map entry or skip all ahead
// tables as a group. Recovery remains bounded and terminal-only, with no regular
// COMMIT controls emitted into the microbatch publisher.
func (p *Postgres) streamCopyOnWriteChanges(ctx context.Context, saved waljs.WALState, metadata map[string]any, callback abstract.CDCMsgFn) (any, error) {
	baseline, err := pglogrepl.ParseLSN(saved.LSN)
	if err != nil || baseline == 0 || len(metadata) != len(p.streams) {
		return nil, fmt.Errorf("copy-on-write requires a valid saved GLOBAL checkpoint and all selected table metadata")
	}
	config, err := p.prepareWALJSConfig(p.streams...)
	if err != nil {
		return nil, err
	}
	config.CommitAligned = true
	config.ReplayAfter = make(map[string]pglogrepl.LSN, len(metadata))
	target := baseline
	for _, stream := range p.streams {
		if _, exists := metadata[stream.ID()]; !exists {
			return nil, fmt.Errorf("copy-on-write missing table checkpoint for %s", stream.ID())
		}
		position, err := copyOnWriteTablePosition(metadata[stream.ID()], baseline)
		if err != nil {
			return nil, fmt.Errorf("copy-on-write table %s: %w", stream.ID(), err)
		}
		config.ReplayAfter[stream.ID()] = position
		target = max(target, position)
	}
	slot, err := waljs.GetSlotPosition(ctx, p.client, p.cdcConfig.ReplicationSlot)
	if err != nil {
		return nil, err
	}
	if slot.Plugin != "pgoutput" {
		return nil, fmt.Errorf("copy-on-write requires pgoutput")
	}
	if slot.LSN > target {
		return nil, fmt.Errorf("copy-on-write slot is ahead of every authoritative table checkpoint")
	}
	for stream, position := range config.ReplayAfter {
		if slot.LSN < baseline && (metadata[stream] == nil || metadata[stream] == "") {
			return nil, fmt.Errorf("copy-on-write state-ahead-slot recovery requires an explicit table checkpoint for %s", stream)
		}
		if position < slot.LSN {
			return nil, fmt.Errorf("copy-on-write table %s does not cover acknowledged slot %s", stream, slot.LSN)
		}
	}
	var recoveryTarget *pglogrepl.LSN
	if target != baseline || slot.LSN != baseline {
		identity, err := p.identifyOrdinaryRecovery(ctx, slot)
		if err != nil {
			return nil, err
		}
		config.StrictRecovery = true
		config.ExpectedSourceIdentity = identity
		recoveryTarget = &target
	} else {
		config.EmitCommitEvents = true
	}
	p.replicator, err = waljs.NewReplicator(ctx, config, slot, recoveryTarget, p.dataTypeConverter, p.columnType)
	if err != nil {
		return nil, err
	}
	if err := p.streamChangesWithToast(ctx, callback); err != nil {
		return nil, err
	}
	socket := p.replicator.Socket()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if socket.TransactionOpen || recoveryTarget != nil && socket.ClientXLogPos != target {
		return nil, fmt.Errorf("copy-on-write replay did not reach its complete transaction boundary")
	}
	return waljs.WALState{LSN: socket.ClientXLogPos.String()}, nil
}

func copyOnWriteTablePosition(raw any, baseline pglogrepl.LSN) (pglogrepl.LSN, error) {
	if raw == nil {
		return baseline, nil
	}
	encoded, ok := raw.(string)
	if !ok {
		return 0, fmt.Errorf("invalid table checkpoint type")
	}
	// Completed initial backfill has no CDC state yet, represented as an empty
	// MetadataState.State string. It starts from the saved snapshot boundary.
	if encoded == "" {
		return baseline, nil
	}
	var state waljs.WALState
	if err := json.Unmarshal([]byte(encoded), &state); err != nil {
		return 0, fmt.Errorf("invalid table checkpoint: %w", err)
	}
	position, err := pglogrepl.ParseLSN(state.LSN)
	if err != nil || position < baseline {
		return 0, fmt.Errorf("table does not cover saved checkpoint %s", baseline)
	}
	return position, nil
}
