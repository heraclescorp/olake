package driver

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"sync"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/drivers/postgres/internal/waljs"
	"github.com/datazip-inc/olake/types"
	"github.com/jackc/pglogrepl"
	"github.com/stretchr/testify/require"
)

func ordinaryTestStreams() []types.StreamInterface {
	var streams []types.StreamInterface
	for _, name := range []string{"first", "second", "third"} {
		stream := types.NewStream(name, "public", nil).Wrap(0)
		stream.Stream.SyncMode = types.CDC
		stream.Stream.SourceDefinedPrimaryKey.Insert("id")
		streams = append(streams, stream)
	}
	streams[1].GetStream().SyncMode = types.STRICTCDC
	return streams
}

func TestOrdinaryRecoveryPlanUsesDeterministicMaximumAndEveryTable(t *testing.T) {
	streams := ordinaryTestStreams()
	for _, order := range [][]int{{0, 1, 2}, {2, 0, 1}, {1, 2, 0}} {
		positions := make(map[string]string)
		values := []string{"0/100", "0/500", "0/300"}
		for _, index := range order {
			positions[streams[index].ID()] = values[index]
		}
		plan, err := ordinaryRecoveryPlan(streams, "0/100", positions, 0x100)
		require.NoError(t, err)
		require.Equal(t, "0/500", plan.TargetLSN)
		require.Equal(t, positions, plan.ReplayAfter, "an ahead table must not skip another table's missing prefix")
		require.Len(t, plan.ReplayAfter, len(streams))
	}
}

func TestOrdinaryRecoveryAcceptsEquivalentLowerSpellings(t *testing.T) {
	for _, lower := range []string{"0/abc", "0/ABC", "000/000ABC"} {
		t.Run(lower, func(t *testing.T) {
			streams := ordinaryTestStreams()
			p, _ := newMockPostgres(t)
			p.CDCSupport = true
			p.cdcConfig.ReplicationSlot = "saved"
			saved := waljs.WALState{LSN: "000/000aBc"}
			p.state = &types.State{RWMutex: &sync.RWMutex{}, Type: types.GlobalType, SuppressPersistence: true,
				Global: &types.GlobalState{State: saved, Streams: types.NewSet(streams[0].ID(), streams[1].ID(), streams[2].ID())}}
			require.NoError(t, p.validateOrdinaryRecoveryCatalog(streams, lower))
			require.Equal(t, saved, p.state.Global.State, "validation must not rewrite the frozen checkpoint")
			positions := map[string]string{streams[0].ID(): "0/ABC", streams[1].ID(): "0/abc", streams[2].ID(): "000/000ABC"}
			plan, err := ordinaryRecoveryPlan(streams, lower, positions, 0xABC)
			require.NoError(t, err)
			require.Equal(t, "0/ABC", plan.LowerLSN)
			require.Equal(t, "0/ABC", plan.TargetLSN)
			for _, stream := range streams {
				require.Equal(t, "0/ABC", plan.ReplayAfter[stream.ID()])
			}
			require.Equal(t, "0/abc", positions[streams[1].ID()], "plan normalization must not rewrite input bookmarks")
		})
	}
}

func TestOrdinaryRecoveryRejectsMalformedLowerSpellings(t *testing.T) {
	streams := ordinaryTestStreams()
	positions := map[string]string{streams[0].ID(): "0/ABC", streams[1].ID(): "0/ABC", streams[2].ID(): "0/ABC"}
	for _, lower := range []string{"", "invalid", "0/0", "0/ABCjunk", "0/ABC/1", "100000000/ABC", "0/100000ABC", " 0/ABC", "0/ABC "} {
		t.Run(lower, func(t *testing.T) {
			_, err := ordinaryRecoveryPlan(streams, lower, positions, 0xABC)
			require.Error(t, err)
			p, _ := newMockPostgres(t)
			p.CDCSupport = true
			p.cdcConfig.ReplicationSlot = "saved"
			p.state = &types.State{RWMutex: &sync.RWMutex{}, Type: types.GlobalType, SuppressPersistence: true,
				Global: &types.GlobalState{State: waljs.WALState{LSN: lower}, Streams: types.NewSet(streams[0].ID(), streams[1].ID(), streams[2].ID())}}
			require.Error(t, p.validateOrdinaryRecoveryCatalog(streams, "0/ABC"), "malformed frozen checkpoints must also fail closed")
			require.Error(t, p.validateOrdinaryRecoveryCatalog(streams, lower))
		})
	}
}

func TestCaptureBackfillRecoveryTargetValidatesNumericOriginalLower(t *testing.T) {
	for _, lower := range []string{"0/abc", "0/ABC", "000/000ABC", "0/ABD", "0/ABB", "", "invalid", "0/0", "0/ABCjunk", "0/ABC/1", "100000000/ABC", "0/100000ABC", " 0/ABC", "0/ABC "} {
		t.Run(lower, func(t *testing.T) {
			p, mock := newMockPostgres(t)
			p.cdcConfig.ReplicationSlot = "saved"
			p.ordinary = &ordinaryRecovery{identity: "validated", lower: 0xABC}
			options := &types.OrdinaryRecoveryOptions{SourceIdentity: "validated", BackfillComplete: true}
			options.Origin.Backfill = true
			ctx := types.WithOrdinaryRecovery(context.Background(), options)
			valid := lower == "0/abc" || lower == "0/ABC" || lower == "000/000ABC"
			slotRead := errors.New("target capture reached validated slot lookup")
			if valid {
				mock.ExpectQuery(ordinaryRecoverySlotQuery).WithArgs("saved").WillReturnError(slotRead)
			}
			target, err := p.CaptureBackfillRecoveryTarget(ctx, lower)
			require.Empty(t, target)
			if valid {
				require.ErrorIs(t, err, slotRead, "equivalent spelling must pass the capture boundary guard")
			} else {
				require.ErrorContains(t, err, "requires the original lower LSN")
			}
		})
	}
}

func TestOrdinaryRecoveryPlanRejectsPartialCoverageAndUnsafeSlots(t *testing.T) {
	streams := ordinaryTestStreams()
	base := map[string]string{streams[0].ID(): "0/100", streams[1].ID(): "0/500", streams[2].ID(): "0/300"}
	for _, scenario := range []string{"missing", "below_baseline", "malformed", "extra", "slot_below_baseline", "slot_ahead_target", "slot_ahead_uncovered", "duplicate"} {
		t.Run(scenario, func(t *testing.T) {
			positions := maps.Clone(base)
			selected := streams
			slot := pglogrepl.LSN(0x100)
			switch scenario {
			case "missing":
				delete(positions, streams[0].ID())
			case "below_baseline":
				positions[streams[0].ID()] = "0/FF"
			case "malformed":
				positions[streams[0].ID()] = "invalid"
			case "extra":
				positions["public.not_selected"] = "0/900"
			case "slot_below_baseline":
				slot = 0xFF
			case "slot_ahead_target":
				slot = 0x600
			case "slot_ahead_uncovered":
				slot = 0x200
			case "duplicate":
				selected = []types.StreamInterface{streams[0], streams[0], streams[2]}
			}
			_, err := ordinaryRecoveryPlan(selected, "0/100", positions, slot)
			require.Error(t, err)
		})
	}
}

func TestOrdinaryRecoveryPlanAcceptsBaselineAndAckBeforeStateRetries(t *testing.T) {
	streams := ordinaryTestStreams()
	for _, scenario := range []struct {
		positions []string
		slot      pglogrepl.LSN
		target    string
	}{
		{[]string{"0/100", "0/100", "0/100"}, 0x100, "0/100"},
		{[]string{"0/300", "0/500", "0/400"}, 0x300, "0/500"},
		{[]string{"0/500", "0/500", "0/500"}, 0x500, "0/500"},
	} {
		positions := make(map[string]string)
		for index, stream := range streams {
			positions[stream.ID()] = scenario.positions[index]
		}
		plan, err := ordinaryRecoveryPlan(streams, "0/100", positions, scenario.slot)
		require.NoError(t, err)
		require.Equal(t, scenario.target, plan.TargetLSN)
	}
}

func TestBackfillRecoveryTargetPreservesCapturedBoundary(t *testing.T) {
	boundary, err := backfillRecoveryTarget("0/500", 0x300, 0x900)
	require.NoError(t, err)
	require.Equal(t, pglogrepl.LSN(0x500), boundary, "fresh WAL must not widen the captured transaction boundary")
	for _, raw := range []string{"", "invalid", "0/0", "0/200", "0/A00"} {
		_, err := backfillRecoveryTarget(raw, 0x300, 0x900)
		require.Error(t, err, raw)
	}
}

func TestBackfillRecoveryRequiresOriginalRetainedLower(t *testing.T) {
	for _, tc := range []struct {
		name      string
		confirmed string
		restart   string
		valid     bool
	}{
		{"original", "0/100", "0/80", true},
		{"exact_restart", "0/100", "0/100", true},
		{"advanced", "0/200", "0/80", false},
		{"behind", "0/90", "0/80", false},
		{"missing_wal", "0/100", "0/110", false},
		{"missing_restart", "0/100", "", false},
		{"zero_restart", "0/100", "0/0", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateBackfillRecoverySlot(ordinaryRecoverySlot{Confirmed: tc.confirmed, Restart: tc.restart}, 0x100)
			if tc.valid {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestBackfillRecoveryCatalogAllowsOnlyIncompleteCDC(t *testing.T) {
	streams := ordinaryTestStreams()
	for _, stream := range streams {
		stream.GetStream().SyncMode = types.CDC
	}
	state := &types.State{RWMutex: &sync.RWMutex{}, Type: types.GlobalType, SuppressPersistence: true,
		Global: &types.GlobalState{State: waljs.WALState{LSN: "0/100"}, Streams: types.NewSet[string]()}}
	p := &Postgres{config: &Config{}, CDCSupport: true, state: state, cdcConfig: CDC{ReplicationSlot: "saved"}}
	require.Error(t, p.validateOrdinaryRecoveryCatalog(streams, "0/100"))
	require.NoError(t, p.validateRecoveryCatalog(streams, "0/100", true))
	streams[0].GetStream().SyncMode = types.STRICTCDC
	require.Error(t, p.validateRecoveryCatalog(streams, "0/100", true))
	streams[0].GetStream().SyncMode = types.CDC
	require.Error(t, p.validateRecoveryCatalog(streams, "0/200", true))
}

func TestOrdinaryRecoveryRequiresEverySelectedSavedGlobalMember(t *testing.T) {
	for _, scenario := range []string{"valid", "missing_global", "stream_state", "missing_member", "extra_member", "fresh_stream", "wrong_baseline", "no_primary_key", "resync"} {
		t.Run(scenario, func(t *testing.T) {
			streams := ordinaryTestStreams()
			state := &types.State{RWMutex: &sync.RWMutex{}, Type: types.GlobalType, SuppressPersistence: true,
				Global: &types.GlobalState{State: waljs.WALState{LSN: "0/100"}, Streams: types.NewSet(streams[0].ID(), streams[1].ID(), streams[2].ID())}}
			p := &Postgres{config: &Config{}, CDCSupport: true, state: state, cdcConfig: CDC{ReplicationSlot: "saved"}}
			switch scenario {
			case "missing_global":
				state.Global = nil
			case "stream_state":
				state.Type = types.StreamType
			case "missing_member":
				state.Global.Streams.Remove(streams[0].ID())
			case "extra_member":
				state.Global.Streams.Insert("public.removed")
			case "fresh_stream":
				streams[0].GetStream().SyncMode = types.FULLREFRESH
			case "wrong_baseline":
				state.Global.State = waljs.WALState{LSN: "0/90"}
			case "no_primary_key":
				streams[0].GetStream().SourceDefinedPrimaryKey = nil
			case "resync":
				p.resync = &resyncSnapshot{}
			}
			err := p.validateOrdinaryRecoveryCatalog(streams, "0/100")
			if scenario == "valid" || scenario == "extra_member" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestOrdinaryRecoverySlotIdentityAndWALPreconditions(t *testing.T) {
	base := ordinaryRecoverySlot{Database: "saved_db", ConnectedDatabase: "saved_db", SlotType: "logical", Plugin: "pgoutput", Confirmed: "0/100", Restart: "0/80", Current: "0/500", WALStatus: "reserved"}
	for _, scenario := range []string{"valid", "database", "connected_database", "plugin", "physical", "active", "wal_lost", "wal_unreserved", "invalidated", "no_restart", "restart_ahead", "below_baseline", "source_behind", "wrong_sender", "dead_sender", "own_sender"} {
		t.Run(scenario, func(t *testing.T) {
			slot := base
			var owned int64
			switch scenario {
			case "database":
				slot.Database = "other"
			case "connected_database":
				slot.ConnectedDatabase = "other"
			case "plugin":
				slot.Plugin = "test_decoding"
			case "physical":
				slot.SlotType = "physical"
			case "active":
				slot.Active = true
			case "wal_lost":
				slot.WALStatus = "lost"
			case "wal_unreserved":
				slot.WALStatus = "unreserved"
			case "invalidated":
				slot.InvalidationReason = "wal_removed"
			case "no_restart":
				slot.Restart = ""
			case "restart_ahead":
				slot.Restart = "0/200"
			case "below_baseline":
				slot.Confirmed = "0/90"
			case "source_behind":
				slot.Current = "0/90"
			case "wrong_sender":
				slot.Active, slot.ActivePID, owned = true, 42, 43
			case "dead_sender":
				owned = 42
			case "own_sender":
				slot.Active, slot.ActivePID, owned = true, 42, 42
			}
			_, err := validateOrdinaryRecoverySlot(slot, "saved_db", "saved_publication", 0x100, owned)
			if scenario == "valid" || scenario == "own_sender" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
	_, err := validateOrdinaryRecoverySlot(base, "saved_db", "", 0x100, 0)
	require.ErrorContains(t, err, "publication")
	base.Plugin = "wal2json"
	_, err = validateOrdinaryRecoverySlot(base, "saved_db", "saved_publication", 0x100, 0)
	require.NoError(t, err, "a publication is ignored by the existing wal2json configuration")
}

func TestOrdinaryRecoveryCheckpointPreservesFullStateAndNumbers(t *testing.T) {
	original := map[string]any{"lsn": "0/100", "opaque": json.Number("9007199254740993"), "nested": map[string]any{"saved": true}}
	checkpoint, err := ordinaryRecoveryCheckpoint(original, "0/500")
	require.NoError(t, err)
	require.Equal(t, "0/100", original["lsn"])
	require.Equal(t, "0/500", checkpoint["lsn"])
	require.Equal(t, original["opaque"], checkpoint["opaque"])
	require.Equal(t, original["nested"], checkpoint["nested"])
}

func TestOrdinaryRecoveryAcknowledgementRejectsUnpreparedChangedAndIncompletePlans(t *testing.T) {
	plan := &types.OrdinaryRecoveryPlan{LowerLSN: "0/100", TargetLSN: "0/500", ReplayAfter: map[string]string{"public.first": "0/100"}}
	ctx := types.WithOrdinaryRecovery(context.Background(), &types.OrdinaryRecoveryOptions{SourceIdentity: "validated", Plan: plan})
	p := &Postgres{}
	require.ErrorContains(t, p.AcknowledgeOrdinaryRecovery(ctx, plan), "prepared plan")
	p.ordinary = &ordinaryRecovery{identity: "validated", lower: 0x100, plan: plan}
	require.ErrorContains(t, p.AcknowledgeOrdinaryRecovery(ctx, plan), "incomplete")
	changed := &types.OrdinaryRecoveryPlan{LowerLSN: plan.LowerLSN, TargetLSN: "0/600", ReplayAfter: plan.ReplayAfter}
	require.ErrorContains(t, p.AcknowledgeOrdinaryRecovery(ctx, changed), "prepared plan")
	changedIdentity := types.WithOrdinaryRecovery(context.Background(), &types.OrdinaryRecoveryOptions{SourceIdentity: "different-live-server", Plan: plan})
	require.ErrorContains(t, p.AcknowledgeOrdinaryRecovery(changedIdentity, plan), "prepared plan")
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	require.ErrorIs(t, p.AcknowledgeOrdinaryRecovery(canceled, plan), context.Canceled)
	p.replicator = resyncCheckpointReplicator{socket: &waljs.Socket{ClientXLogPos: 0x500}}
	require.ErrorContains(t, p.PostCDC(ctx, 0), "explicit acknowledgement")
	require.NotNil(t, p.replicator, "only explicit recovery cleanup owns the WAL sender")
	require.NoError(t, p.CleanupOrdinaryRecovery(canceled))
	require.Nil(t, p.replicator)
	require.Nil(t, p.ordinary)
}

func TestOrdinaryRecoveryPreservesConfiguredSourceToastSemantics(t *testing.T) {
	for _, missing := range []bool{false, true} {
		t.Run(map[bool]string{false: "current_source_lookup", true: "missing_source_row"}[missing], func(t *testing.T) {
			p, mock := newMockPostgres(t)
			p.typeMapping = pgTypeToDataTypes()
			p.replicator = &toastTestReplicator{changes: []abstract.CDCChange{toastChange(1)}, socket: waljs.Socket{StrictRecovery: true, ClientXLogPos: 0x500, CurrentWalPosition: 0x500}}
			rows := sqlmock.NewRowsWithColumnDefinition(sqlmock.NewColumn("id").OfType("INT8", int64(0)), sqlmock.NewColumn("doc").OfType("TEXT", ""))
			if !missing {
				rows.AddRow(int64(1), "current source value, potentially newer than target")
			}
			mock.ExpectQuery(`SELECT "id", "doc" FROM "public"."users" WHERE ("id" = $1)`).WithArgs(int64(1)).WillReturnRows(rows)
			ctx := types.WithOrdinaryRecovery(context.Background(), &types.OrdinaryRecoveryOptions{})
			emitted := false
			err := p.streamChangesWithToast(ctx, func(_ context.Context, change abstract.CDCChange) error {
				emitted = true
				require.Empty(t, change.UnavailableColumns)
				require.Equal(t, "current source value, potentially newer than target", change.Data["doc"])
				return nil
			})
			if missing {
				require.Error(t, err)
				require.False(t, emitted)
			} else {
				require.NoError(t, err)
				require.True(t, emitted)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
