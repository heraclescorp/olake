package iceberg

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	apacheiceberg "github.com/apache/iceberg-go"
	icesql "github.com/apache/iceberg-go/catalog/sql"
	"github.com/apache/iceberg-go/table"
	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/stretchr/testify/require"
)

func TestPartialBackfillExportedCLISmoke(t *testing.T) {
	for _, strategy := range []types.MergeStrategy{types.MergeOnRead, types.CopyOnWrite} {
		t.Run(string(strategy), func(t *testing.T) {
			partialBackfillExportedCLISmoke(t, strategy)
		})
	}
}

func partialBackfillExportedCLISmoke(t *testing.T, strategy types.MergeStrategy) {
	binaryPath, dsn := os.Getenv("OLAKE_ORDINARY_RECOVERY_CLI"), os.Getenv("OLAKE_POSTGRES_TEST_DSN")
	if binaryPath == "" || dsn == "" {
		t.Skip("requires compiled PostgreSQL CLI and disposable logical PostgreSQL")
	}
	binaryPath, err := filepath.Abs(binaryPath)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	db, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	defer db.Close()
	id := "backfill_cli_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	namespace, publication, slot := id, id+"_pub", id+"_slot"
	_, err = db.ExecContext(ctx, fmt.Sprintf(`CREATE SCHEMA %q; CREATE TABLE %q.a(id bigint PRIMARY KEY,value bigint); CREATE TABLE %q.b(id bigint PRIMARY KEY,value bigint); CREATE TABLE %q.c(id bigint PRIMARY KEY,value bigint); INSERT INTO %q.a VALUES(1,10),(2,20); INSERT INTO %q.b VALUES(1,10),(2,20); CREATE PUBLICATION %q FOR TABLE %q.a,%q.b,%q.c`, namespace, namespace, namespace, namespace, namespace, namespace, publication, namespace, namespace, namespace))
	require.NoError(t, err)
	defer func() {
		_, _ = db.ExecContext(context.Background(), "SELECT pg_drop_replication_slot($1) WHERE EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)", slot)
		_, _ = db.ExecContext(context.Background(), fmt.Sprintf(`DROP PUBLICATION IF EXISTS %q; DROP SCHEMA IF EXISTS %q CASCADE`, publication, namespace))
	}()
	var slotStart string
	require.NoError(t, db.QueryRowContext(ctx, "SELECT lsn::text FROM pg_create_logical_replication_slot($1,'pgoutput')", slot).Scan(&slotStart))
	root := t.TempDir()
	t.Cleanup(func() {
		if !t.Failed() {
			return
		}
		_ = filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
			if err == nil && !entry.IsDir() && entry.Name() == "olake.log" {
				data, readErr := os.ReadFile(path)
				if readErr == nil {
					t.Logf("CLI log %s:\n%s", path, data)
				}
			}
			return nil
		})
	})
	warehouse := filepath.Join(root, "warehouse")
	require.NoError(t, os.MkdirAll(warehouse, 0700))
	cat, err := icesql.NewCatalog(id, db, icesql.Postgres, apacheiceberg.Properties{"warehouse": warehouse})
	require.NoError(t, err)
	defer cat.Close()
	require.NoError(t, cat.CreateNamespace(ctx, table.Identifier{namespace}, nil))
	defer func() {
		_ = cat.DropTable(context.Background(), table.Identifier{namespace, "a"})
		_ = cat.DropTable(context.Background(), table.Identifier{namespace, "b"})
		_ = cat.DropTable(context.Background(), table.Identifier{namespace, "c"})
		_ = cat.DropNamespace(context.Background(), table.Identifier{namespace})
	}()
	var injected atomic.Bool
	interrupted := make(chan struct{})
	var once sync.Once
	committedTable := make(chan string, 1)
	var recoveryFault atomic.Bool
	var recoveryOnce sync.Once
	recoveryInterrupted := make(chan struct{})
	capturedTargets := make(chan string, 1)
	server := ordinaryRecoverySQLREST(t, cat, func(requestCtx context.Context, committed bool) {
		if !committed {
			return
		}
		if injected.Load() {
			if !recoveryFault.Load() {
				return
			}
			for _, name := range []string{"a", "b", "c"} {
				current, err := cat.LoadTable(requestCtx, table.Identifier{namespace, name})
				if err != nil {
					continue
				}
				var marker ordinaryRecoveryMarker
				if json.Unmarshal([]byte(current.Properties()[ordinaryRecoveryProperty]), &marker) != nil || marker.BackfillTargetLSN == "" {
					continue
				}
				recoveryOnce.Do(func() { capturedTargets <- marker.BackfillTargetLSN; close(recoveryInterrupted); <-requestCtx.Done() })
			}
			return
		}
		for _, name := range []string{"a", "b"} {
			current, err := cat.LoadTable(requestCtx, table.Identifier{namespace, name})
			if err != nil {
				continue
			}
			var metadata types.MetadataState
			if json.Unmarshal([]byte(current.Properties()[nativeRecoveryProperty]), &metadata) != nil || len(metadata.FullRefreshCommittedIDs) == 0 {
				continue
			}
			once.Do(func() { committedTable <- name; close(interrupted); <-requestCtx.Done() })
		}
	})
	defer server.Close()
	runtime := filepath.Join(root, "original")
	require.NoError(t, os.MkdirAll(runtime, 0700))
	parsed, err := url.Parse(dsn)
	require.NoError(t, err)
	port := 5432
	if parsed.Port() != "" {
		port, err = strconv.Atoi(parsed.Port())
		require.NoError(t, err)
	}
	password, _ := parsed.User.Password()
	sslmode := parsed.Query().Get("sslmode")
	if sslmode == "" {
		sslmode = "disable"
	}
	source := map[string]any{"host": parsed.Hostname(), "port": port, "database": strings.TrimPrefix(parsed.Path, "/"), "username": parsed.User.Username(), "password": password, "ssl": map[string]any{"mode": sslmode}, "max_threads": 1, "retry_count": 1, "update_method": map[string]any{"replication_slot": slot, "publication": publication, "initial_wait_time": 10, "toast_value_source": "source"}}
	destination := map[string]any{"type": "ICEBERG", "writer": map[string]any{"catalog_type": "rest", "catalog_name": id, "rest_catalog_url": server.URL, "rest_auth_type": "none", "iceberg_db": namespace, "iceberg_s3_path": warehouse, "aws_region": "us-east-1"}}
	saved := &types.Catalog{SelectedStreams: map[string][]types.StreamMetadata{}}
	for _, name := range []string{"a", "b", "c"} {
		stream := types.NewStream(name, namespace, nil)
		stream.SyncMode = types.CDC
		stream.DestinationDatabase, stream.DestinationTable = namespace, name
		stream.SourceDefinedPrimaryKey = types.NewSet("id")
		stream.UpsertField("id", types.Int64, false, false)
		stream.UpsertField("value", types.Int64, true, false)
		stream.UpsertField(constants.OlakeID, types.String, false, true)
		stream.UpsertField(constants.OpType, types.String, false, true)
		stream.UpsertField(constants.OlakeTimestamp, types.TimestampMicro, false, true)
		stream.UpsertField(constants.CdcTimestamp, types.TimestampMicro, true, true)
		stream.UpsertField("_cdc_lsn", types.String, true, true)
		normalized, appendMode, update := true, false, string(types.UpdateTypeEquality)
		metadata := types.StreamMetadata{StreamName: name, Normalization: &normalized, AppendMode: &appendMode, UpdateType: &update, MergeStrategy: strategy, UseSourceColumnNames: true}
		saved.Streams = append(saved.Streams, &types.ConfiguredStream{Stream: stream, StreamMetadata: metadata})
		saved.SelectedStreams[namespace] = append(saved.SelectedStreams[namespace], metadata)
	}
	write := func(name string, value any) []byte {
		data, err := json.Marshal(value)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(runtime, name), data, 0600))
		return data
	}
	artifacts := map[string][]byte{
		"source.json": write("source.json", source), "destination.json": write("destination.json", destination),
		"available_streams.json": write("available_streams.json", &types.Catalog{Streams: saved.Streams}),
		"selected_streams.json":  write("selected_streams.json", &types.Catalog{SelectedStreams: saved.SelectedStreams}),
	}
	originalState := write("state.json", map[string]any{})
	originalID, executionID := "original-"+id, "original-"+id
	write("execution-fence.json", map[string]any{"version": 1, "execution_id": executionID, "state": "open"})
	command := func(recovery bool) *exec.Cmd {
		args := []string{"sync", "--config", filepath.Join(runtime, "source.json"), "--destination", filepath.Join(runtime, "destination.json"), "--available-streams", filepath.Join(runtime, "available_streams.json"), "--selected-streams", filepath.Join(runtime, "selected_streams.json"), "--state", filepath.Join(runtime, "state.json"), "--execution-id", executionID, "--execution-fence", filepath.Join(runtime, "execution-fence.json")}
		if recovery {
			args = append(args, "--execution-launch-id", originalID, "--ordinary-recovery-origin", filepath.Join(runtime, "ordinary-recovery-origin.json"), "--ordinary-recovery-result", filepath.Join(runtime, "ordinary-recovery-result.json"))
			args = append(args, "--backfill-recovery-checkpoint", filepath.Join(runtime, "backfill-recovery-checkpoint.json"))
		}
		child := exec.CommandContext(ctx, binaryPath, args...)
		child.Dir = runtime
		for _, entry := range os.Environ() {
			key, _, _ := strings.Cut(entry, "=")
			if key != constants.EnvS3ConfigFolder && key != constants.EnvStorageMode && key != "TELEMETRY_DISABLED" {
				child.Env = append(child.Env, entry)
			}
		}
		child.Env = append(child.Env, "TELEMETRY_DISABLED=true", "AWS_EC2_METADATA_DISABLED=true")
		return child
	}
	child := command(false)
	var output bytes.Buffer
	child.Stdout, child.Stderr = &output, &output
	require.NoError(t, child.Start())
	finished := make(chan error, 1)
	go func() { finished <- child.Wait() }()
	select {
	case <-interrupted:
	case err := <-finished:
		t.Fatalf("original CLI exited before committed backfill fault: %v\n%s", err, output.String())
	case <-ctx.Done():
		_ = child.Process.Kill()
		<-finished
		t.Fatalf("original CLI did not commit backfill\n%s", output.String())
	}
	require.NoError(t, child.Process.Kill())
	require.Error(t, <-finished)
	injected.Store(true)
	committedName := <-committedTable
	readJournals := func(directory string) map[string][]byte {
		paths, err := filepath.Glob(filepath.Join(directory, "backfill-recovery-binding-*.json"))
		require.NoError(t, err)
		journals := make(map[string][]byte, len(paths))
		for _, path := range paths {
			data, err := os.ReadFile(path)
			require.NoError(t, err)
			var binding types.BackfillRecoveryBinding
			require.NoError(t, json.Unmarshal(data, &binding))
			digest := sha256.Sum256([]byte(binding.Stream))
			require.Equal(t, "backfill-recovery-binding-"+hex.EncodeToString(digest[:])+".json", filepath.Base(path))
			journals[filepath.Base(path)] = data
		}
		return journals
	}
	inheritJournals := func(directory string, journals map[string][]byte) {
		for name, data := range journals {
			path := filepath.Join(directory, name)
			file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
			if os.IsExist(err) {
				existing, readErr := os.ReadFile(path)
				require.NoError(t, readErr)
				require.Equal(t, data, existing, "inherited journal merge must reject conflicting immutable bytes")
				continue
			}
			require.NoError(t, err)
			_, err = file.Write(data)
			require.NoError(t, err)
			require.NoError(t, file.Close())
		}
	}
	originalJournals := readJournals(runtime)
	require.NotEmpty(t, originalJournals)
	require.Less(t, len(originalJournals), 3, "fault must leave at least one selected table unstarted")
	candidateBytes, err := os.ReadFile(filepath.Join(runtime, "backfill-recovery-checkpoint.json"))
	require.NoError(t, err)
	var candidate types.BackfillRecoveryCheckpoint
	require.NoError(t, json.Unmarshal(candidateBytes, &candidate))
	candidateState := types.State{RWMutex: &sync.RWMutex{}}
	require.NoError(t, json.Unmarshal(candidate.State, &candidateState))
	var lowerCheckpoint struct {
		LSN string `json:"lsn"`
	}
	require.NoError(t, utils.Unmarshal(candidateState.GetGlobal().State, &lowerCheckpoint))
	for _, stream := range saved.Streams {
		if stream.Name() == committedName {
			plan := candidateState.GetBackfillPlan(stream)
			require.NotNil(t, plan)
			require.Equal(t, 1, plan.Len(), "fault must interrupt the sole, open-ended tail chunk")
			require.False(t, candidateState.HasCompletedBackfill(stream))
		}
	}
	var confirmed string
	require.NoError(t, db.QueryRowContext(ctx, "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name=$1", slot).Scan(&confirmed))
	require.Equal(t, lowerCheckpoint.LSN, confirmed)
	before, err := cat.LoadTable(ctx, table.Identifier{namespace, committedName})
	require.NoError(t, err)
	originalRuntime := runtime
	write("execution-fence.json", map[string]any{"version": 1, "execution_id": executionID, "state": "closed"})
	_, err = db.ExecContext(ctx, fmt.Sprintf(`UPDATE %q.a SET value=11 WHERE id=1; UPDATE %q.b SET value=11 WHERE id=1; DELETE FROM %q.a WHERE id=2; DELETE FROM %q.b WHERE id=2; INSERT INTO %q.a VALUES(3,30); INSERT INTO %q.b VALUES(3,30)`, namespace, namespace, namespace, namespace, namespace, namespace))
	require.NoError(t, err)
	runtime = filepath.Join(root, "recovery")
	require.NoError(t, os.MkdirAll(runtime, 0700))
	for name, data := range artifacts {
		require.NoError(t, os.WriteFile(filepath.Join(runtime, name), data, 0600))
	}
	require.NoError(t, os.WriteFile(filepath.Join(runtime, "state.json"), candidate.State, 0600))
	require.NoError(t, os.WriteFile(filepath.Join(runtime, "backfill-recovery-checkpoint.json"), candidateBytes, 0600))
	inheritJournals(runtime, originalJournals)
	require.Equal(t, originalJournals, readJournals(runtime))
	catalogHash := sha256.New()
	for _, name := range []string{"available_streams.json", "selected_streams.json"} {
		data := artifacts[name]
		fmt.Fprintf(catalogHash, "%s%c%d%c", name, 0, len(data), 0)
		catalogHash.Write(data)
	}
	savedHash, err := utils.CanonicalStateSHA256(originalState)
	require.NoError(t, err)
	origin := types.OrdinaryRecoveryOrigin{Version: 1, OriginalExecutionID: originalID, OriginalLaunchID: originalID, SourceSHA256: ordinaryRecoverySmokeHash(artifacts["source.json"]), DestinationSHA256: ordinaryRecoverySmokeHash(artifacts["destination.json"]), CatalogSHA256: hex.EncodeToString(catalogHash.Sum(nil)), BaselineStateSHA256: candidate.StateSHA256, Backfill: true, BackfillCheckpointSHA256: ordinaryRecoverySmokeHash(candidateBytes), SavedStateSHA256: savedHash}
	originBytes := write("ordinary-recovery-origin.json", origin)
	executionID = "recovery-" + id
	write("execution-fence.json", map[string]any{"version": 1, "execution_id": executionID, "state": "open"})
	recoveryFault.Store(true)
	recoveryChild := command(true)
	var recoveryLog bytes.Buffer
	recoveryChild.Stdout, recoveryChild.Stderr = &recoveryLog, &recoveryLog
	require.NoError(t, recoveryChild.Start())
	recoveryFinished := make(chan error, 1)
	go func() { recoveryFinished <- recoveryChild.Wait() }()
	select {
	case <-recoveryInterrupted:
	case err := <-recoveryFinished:
		t.Fatalf("recovery exited before persisted target interruption: %v\n%s", err, recoveryLog.String())
	case <-ctx.Done():
		_ = recoveryChild.Process.Kill()
		<-recoveryFinished
		t.Fatalf("recovery never published a target barrier\n%s", recoveryLog.String())
	}
	require.NoError(t, recoveryChild.Process.Kill())
	require.Error(t, <-recoveryFinished)
	recoveryFault.Store(false)
	fixedTarget := <-capturedTargets
	for _, name := range []string{"execution-result.json", "ordinary-recovery-result.json"} {
		_, err := os.Stat(filepath.Join(runtime, name))
		require.True(t, os.IsNotExist(err), "interrupted recovery must not settle")
	}
	for name, want := range map[string][]byte{"ordinary-recovery-origin.json": originBytes, "backfill-recovery-checkpoint.json": candidateBytes} {
		actual, err := os.ReadFile(filepath.Join(runtime, name))
		require.NoError(t, err)
		require.Equal(t, want, actual)
	}
	recoveryJournals := readJournals(runtime)
	require.Len(t, recoveryJournals, 3, "first recovery must durably journal every selected table, including newly started tables")
	for name, want := range originalJournals {
		require.Equal(t, want, recoveryJournals[name], "first recovery must preserve inherited journal bytes")
	}
	newlyStartedJournals := make(map[string][]byte)
	for name, data := range recoveryJournals {
		if _, existed := originalJournals[name]; !existed {
			newlyStartedJournals[name] = data
		}
	}
	require.NotEmpty(t, newlyStartedJournals)
	require.NoError(t, db.QueryRowContext(ctx, "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name=$1", slot).Scan(&confirmed))
	require.Equal(t, lowerCheckpoint.LSN, confirmed, "interrupted recovery must preserve the original acknowledgement")
	afterInterruption, err := cat.LoadTable(ctx, table.Identifier{namespace, committedName})
	require.NoError(t, err)
	interruptedSnapshotCount := len(afterInterruption.Metadata().Snapshots())
	require.LessOrEqual(t, interruptedSnapshotCount, len(before.Metadata().Snapshots())+1, "first recovery may add only a barrier to the already committed tail")
	failedRecoveryRuntime := runtime
	write("execution-fence.json", map[string]any{"version": 1, "execution_id": executionID, "state": "closed"})
	_, err = db.ExecContext(ctx, fmt.Sprintf(`UPDATE %q.a SET value=999 WHERE id=1; UPDATE %q.b SET value=999 WHERE id=1; INSERT INTO %q.a VALUES(4,999); INSERT INTO %q.b VALUES(4,999); INSERT INTO %q.c VALUES(4,999)`, namespace, namespace, namespace, namespace, namespace))
	require.NoError(t, err)
	runtime = filepath.Join(root, "successor")
	require.NoError(t, os.MkdirAll(runtime, 0700))
	for name, data := range artifacts {
		require.NoError(t, os.WriteFile(filepath.Join(runtime, name), data, 0600))
	}
	require.NoError(t, os.WriteFile(filepath.Join(runtime, "state.json"), candidate.State, 0600))
	require.NoError(t, os.WriteFile(filepath.Join(runtime, "backfill-recovery-checkpoint.json"), candidateBytes, 0600))
	inheritJournals(runtime, originalJournals)
	inheritJournals(runtime, recoveryJournals)
	require.Equal(t, recoveryJournals, readJournals(runtime))
	require.Equal(t, originBytes, write("ordinary-recovery-origin.json", origin))
	executionID = "successor-" + id
	write("execution-fence.json", map[string]any{"version": 1, "execution_id": executionID, "state": "open"})
	recoveryOutput, err := command(true).CombinedOutput()
	require.NoError(t, err, "recovery CLI: %s", recoveryOutput)
	data, err := os.ReadFile(filepath.Join(runtime, "ordinary-recovery-result.json"))
	require.NoError(t, err)
	var result types.OrdinaryRecoveryResult
	require.NoError(t, json.Unmarshal(data, &result))
	require.Equal(t, origin, result.OrdinaryRecoveryOrigin)
	require.Equal(t, lowerCheckpoint.LSN, result.LowerLSN)
	require.Equal(t, fixedTarget, result.TargetLSN, "successor must reuse the persisted transaction boundary despite newer source commits")
	require.Len(t, result.Tables, 3)
	for _, proof := range result.Tables {
		require.Equal(t, result.TargetLSN, proof.CommittedLSN)
	}
	require.NoError(t, db.QueryRowContext(ctx, "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name=$1", slot).Scan(&confirmed))
	require.Equal(t, result.TargetLSN, confirmed)
	for _, name := range []string{"a", "b", "c"} {
		current, err := cat.LoadTable(ctx, table.Identifier{namespace, name})
		require.NoError(t, err)
		rows := ordinaryRecoverySmokeRows(t, current)
		if name == "c" {
			require.Empty(t, rows, "selected zero-row backfill must reach the common checkpoint without fabricated records")
			continue
		}
		require.Len(t, rows, 3)
		require.Equal(t, int64(11), rows[utils.GetKeysHash(map[string]any{"id": int64(1)}, "id")].value)
		require.Equal(t, "d", rows[utils.GetKeysHash(map[string]any{"id": int64(2)}, "id")].operation)
		require.Equal(t, int64(30), rows[utils.GetKeysHash(map[string]any{"id": int64(3)}, "id")].value)
		if name == committedName {
			require.Equal(t, before.Metadata().TableUUID(), current.Metadata().TableUUID())
			require.Len(t, current.Metadata().Snapshots(), interruptedSnapshotCount+2, "committed tail must not be reread: only the fresh recovery barrier and bounded CDC may add snapshots")
			for _, snapshot := range before.Metadata().Snapshots() {
				require.NotNil(t, current.Metadata().SnapshotByID(snapshot.SnapshotID))
			}
		}
	}
	require.Equal(t, originalJournals, readJournals(originalRuntime))
	require.Equal(t, recoveryJournals, readJournals(failedRecoveryRuntime))
	require.Equal(t, recoveryJournals, readJournals(runtime), "successor must retain inherited and newly started immutable journals byte-for-byte")
	for name, want := range newlyStartedJournals {
		require.Equal(t, want, readJournals(runtime)[name])
	}
	frozen, err := os.ReadFile(filepath.Join(originalRuntime, "backfill-recovery-checkpoint.json"))
	require.NoError(t, err)
	require.Equal(t, candidateBytes, frozen)
	frozen, err = os.ReadFile(filepath.Join(runtime, "ordinary-recovery-origin.json"))
	require.NoError(t, err)
	require.Equal(t, originBytes, frozen)
	frozen, err = os.ReadFile(filepath.Join(failedRecoveryRuntime, "ordinary-recovery-origin.json"))
	require.NoError(t, err)
	require.Equal(t, originBytes, frozen)
	frozen, err = os.ReadFile(filepath.Join(failedRecoveryRuntime, "backfill-recovery-checkpoint.json"))
	require.NoError(t, err)
	require.Equal(t, candidateBytes, frozen)
	_, err = os.Stat(filepath.Join(originalRuntime, "execution-result.json"))
	require.True(t, os.IsNotExist(err))
}
