package iceberg

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
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

	"github.com/apache/arrow-go/v18/arrow/array"
	apacheiceberg "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	icesql "github.com/apache/iceberg-go/catalog/sql"
	"github.com/apache/iceberg-go/table"
	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/stretchr/testify/require"
)

// TestOrdinaryRecoveryExportedCLISmoke runs the actual compiled PostgreSQL sync
// command against disposable PostgreSQL and a real SQLCatalog behind a local
// REST transport. No metadata or source driver is mocked. Build the postgres
// binary first, then set OLAKE_ORDINARY_RECOVERY_CLI and OLAKE_POSTGRES_TEST_DSN.
func TestOrdinaryRecoveryExportedCLISmoke(t *testing.T) {
	binaryPath, dsn := os.Getenv("OLAKE_ORDINARY_RECOVERY_CLI"), os.Getenv("OLAKE_POSTGRES_TEST_DSN")
	if binaryPath == "" || dsn == "" {
		t.Skip("requires compiled PostgreSQL CLI and disposable logical PostgreSQL")
	}
	absoluteBinary, err := filepath.Abs(binaryPath)
	require.NoError(t, err)
	for _, scenario := range []struct {
		name                string
		mixed               bool
		interruptedBarriers int
		missingReceipt      bool
	}{
		{name: "all_committed", interruptedBarriers: -1},
		{name: "mixed_watermarks", mixed: true, interruptedBarriers: -1},
		{name: "missing_helpers_before_any_barrier", mixed: true, interruptedBarriers: 0},
		{name: "missing_helpers_after_partial_barriers", mixed: true, interruptedBarriers: 1},
		{name: "missing_helpers_after_all_barriers", mixed: true, interruptedBarriers: 2},
		{name: "missing_original_receipt_and_partial_helpers", mixed: true, interruptedBarriers: 1, missingReceipt: true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			ordinaryRecoveryCLISmoke(t, absoluteBinary, dsn, scenario.mixed, scenario.interruptedBarriers, scenario.missingReceipt)
		})
	}
}

func ordinaryRecoveryCLISmoke(t *testing.T, binaryPath, dsn string, mixed bool, interruptedBarriers int, missingReceipt bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	db, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	defer db.Close()
	require.NoError(t, db.PingContext(ctx))
	id := "ordinary_cli_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	namespace, publication, slot := id, id+"_pub", id+"_slot"
	_, err = db.ExecContext(ctx, fmt.Sprintf(`CREATE SCHEMA %q; CREATE TABLE %q.a(id bigint PRIMARY KEY,value bigint); CREATE TABLE %q.b(id bigint PRIMARY KEY,value bigint); INSERT INTO %q.a VALUES(0,0); INSERT INTO %q.b VALUES(0,0); CREATE PUBLICATION %q FOR TABLE %q.a,%q.b`, namespace, namespace, namespace, namespace, namespace, publication, namespace, namespace))
	require.NoError(t, err)
	defer func() {
		_, _ = db.ExecContext(context.Background(), "SELECT pg_drop_replication_slot($1) WHERE EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)", slot)
		_, _ = db.ExecContext(context.Background(), fmt.Sprintf(`DROP PUBLICATION IF EXISTS %q; DROP SCHEMA IF EXISTS %q CASCADE`, publication, namespace))
	}()
	var lower string
	require.NoError(t, db.QueryRowContext(ctx, "SELECT lsn::text FROM pg_create_logical_replication_slot($1,'pgoutput')", slot).Scan(&lower))
	_, err = db.ExecContext(ctx, fmt.Sprintf(`BEGIN;
		INSERT INTO %q.a VALUES(1,10); INSERT INTO %q.b VALUES(1,20);
		UPDATE %q.a SET value=11 WHERE id=1; UPDATE %q.b SET value=21 WHERE id=1;
		COMMIT; BEGIN;
		DELETE FROM %q.a WHERE id=1; DELETE FROM %q.b WHERE id=1;
		INSERT INTO %q.a VALUES(1,10); INSERT INTO %q.b VALUES(1,20);
		UPDATE %q.a SET value=5 WHERE id=0; UPDATE %q.b SET value=5 WHERE id=0;
		DELETE FROM %q.a WHERE id=0; DELETE FROM %q.b WHERE id=0;
		INSERT INTO %q.a VALUES(3,30); INSERT INTO %q.b VALUES(3,30);
		DELETE FROM %q.a WHERE id=3; DELETE FROM %q.b WHERE id=3;
		COMMIT`, namespace, namespace, namespace, namespace, namespace, namespace, namespace, namespace, namespace, namespace, namespace, namespace, namespace, namespace, namespace, namespace))
	require.NoError(t, err)
	target := ordinaryRecoveryPeekCommit(t, ctx, db, slot, publication)
	// A later source transaction exists before recovery. Neither its events nor
	// its checkpoint may be admitted by the bounded sync.
	_, err = db.ExecContext(ctx, fmt.Sprintf(`BEGIN; UPDATE %q.a SET value=999 WHERE id=1; UPDATE %q.b SET value=999 WHERE id=1; INSERT INTO %q.a VALUES(0,999),(2,999); INSERT INTO %q.b VALUES(0,999),(2,999); COMMIT`, namespace, namespace, namespace, namespace))
	require.NoError(t, err)
	root := t.TempDir()
	warehouse := filepath.Join(root, "warehouse")
	require.NoError(t, os.MkdirAll(warehouse, 0700))
	cat, err := icesql.NewCatalog(id, db, icesql.Postgres, apacheiceberg.Properties{"warehouse": warehouse})
	require.NoError(t, err)
	defer cat.Close()
	require.NoError(t, cat.CreateNamespace(ctx, table.Identifier{namespace}, nil))
	defer func() {
		_ = cat.DropTable(context.Background(), table.Identifier{namespace, "a"})
		_ = cat.DropTable(context.Background(), table.Identifier{namespace, "b"})
		_ = cat.DropNamespace(context.Background(), table.Identifier{namespace})
	}()
	saved := &types.Catalog{SelectedStreams: map[string][]types.StreamMetadata{}}
	originals := make(map[string]*table.Table)
	coordinated := &nativeCatalog{Catalog: cat}
	seedConfig := &Config{IcebergDatabase: namespace, IcebergS3Path: warehouse}
	for _, name := range []string{"a", "b"} {
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
		// ProduceSchema always includes this reserved column for CDC-capable
		// PostgreSQL sources; replay must not need to evolve a hand-built fixture.
		stream.UpsertField("_cdc_lsn", types.String, true, true)
		normalized, appendMode, update := true, false, string(types.UpdateTypeEquality)
		metadata := types.StreamMetadata{StreamName: name, Normalization: &normalized, AppendMode: &appendMode, UpdateType: &update, UseSourceColumnNames: true}
		configured := &types.ConfiguredStream{Stream: stream, StreamMetadata: metadata}
		saved.Streams = append(saved.Streams, configured)
		saved.SelectedStreams[namespace] = append(saved.SelectedStreams[namespace], metadata)
		position := target
		if mixed && name == "b" {
			position = lower
		}
		writer := openNativeTestWriter(ctx, t, seedConfig, configured, coordinated, nil, "seed-"+name, true)
		records := []types.RawRecord{ordinaryRecoveryCLIRow(0, 0, lower)}
		if position == target {
			value := int64(10)
			if name == "b" {
				value = 20
			}
			records = []types.RawRecord{ordinaryRecoveryCLIRow(0, 0, target), ordinaryRecoveryCLIRow(3, 0, target)}
			for index := range records {
				records[index].OlakeColumns[constants.OpType] = "d"
				records[index].Data["value"] = nil
			}
			records = append(records, ordinaryRecoveryCLIRow(1, value, target))
		}
		require.NoError(t, writer.Write(ctx, records))
		require.NoError(t, writer.Close(ctx, map[string]any{"state": fmt.Sprintf(`{"lsn":%q}`, position)}))
		originals[name], err = cat.LoadTable(ctx, table.Identifier{namespace, name})
		require.NoError(t, err)
	}
	interrupted := make(chan struct{})
	var stop sync.Once
	var barrierCount atomic.Int64
	server := ordinaryRecoverySQLREST(t, cat, func(requestCtx context.Context, committed bool) {
		if interruptedBarriers < 0 {
			return
		}
		if (committed && barrierCount.Add(1) == int64(interruptedBarriers)) || (!committed && interruptedBarriers == 0) {
			stop.Do(func() {
				close(interrupted)
				<-requestCtx.Done()
			})
		}
	})
	defer server.Close()
	runtime := filepath.Join(root, "runtime")
	originalDir := filepath.Join(root, "original")
	require.NoError(t, os.MkdirAll(runtime, 0700))
	require.NoError(t, os.MkdirAll(originalDir, 0700))
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
	source := map[string]any{"host": parsed.Hostname(), "port": port, "database": strings.TrimPrefix(parsed.Path, "/"), "username": parsed.User.Username(), "password": password, "ssl": map[string]any{"mode": sslmode}, "max_threads": 2, "retry_count": 1, "update_method": map[string]any{"replication_slot": slot, "publication": publication, "initial_wait_time": 10, "toast_value_source": "source"}}
	destination := map[string]any{"type": "ICEBERG", "writer": map[string]any{"catalog_type": "rest", "catalog_name": id, "rest_catalog_url": server.URL, "rest_auth_type": "none", "iceberg_db": namespace, "iceberg_s3_path": warehouse, "aws_region": "us-east-1"}}
	checkpoint := &types.State{RWMutex: &sync.RWMutex{}, Type: types.GlobalType, Version: constants.LatestStateVersion, Global: &types.GlobalState{State: map[string]any{"lsn": lower, "unchanged_large": json.Number("9007199254740993")}, Streams: types.NewSet(namespace+".a", namespace+".b")}, SuppressPersistence: true}
	write := func(name string, value any) []byte {
		data, err := json.Marshal(value)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(runtime, name), data, 0600))
		return data
	}
	sourceBytes := write("source.json", source)
	destinationBytes := write("destination.json", destination)
	availableBytes := write("available_streams.json", &types.Catalog{Streams: saved.Streams})
	selectedBytes := write("selected_streams.json", &types.Catalog{SelectedStreams: saved.SelectedStreams})
	baseline := write("state.json", checkpoint)
	originalID := "original-" + id
	executionID := "recovery-" + id
	var oldReceipt []byte
	originalReceiptPath := filepath.Join(originalDir, "execution-result.json")
	if !missingReceipt {
		oldReceipt = []byte(fmt.Sprintf(`{"version":1,"execution_id":%q,"launch_id":%q,"command":"sync","outcome":"unknown"}`, originalID, originalID))
		require.NoError(t, os.WriteFile(originalReceiptPath, oldReceipt, 0600))
	}
	baselineDigest, err := utils.CanonicalStateSHA256(baseline)
	require.NoError(t, err)
	catalogHash := sha256.New()
	for _, file := range []struct {
		name string
		data []byte
	}{{"available_streams.json", availableBytes}, {"selected_streams.json", selectedBytes}} {
		fmt.Fprintf(catalogHash, "%s%c%d%c", file.name, 0, len(file.data), 0)
		catalogHash.Write(file.data)
	}
	receiptHash := ""
	if !missingReceipt {
		receiptHash = ordinaryRecoverySmokeHash(oldReceipt)
	}
	origin := types.OrdinaryRecoveryOrigin{Version: 1, OriginalExecutionID: originalID, OriginalLaunchID: originalID, OriginalReceiptSHA256: receiptHash, SourceSHA256: ordinaryRecoverySmokeHash(sourceBytes), DestinationSHA256: ordinaryRecoverySmokeHash(destinationBytes), CatalogSHA256: hex.EncodeToString(catalogHash.Sum(nil)), BaselineStateSHA256: baselineDigest}
	originBytes := write("ordinary-recovery-origin.json", origin)
	write("execution-fence.json", map[string]any{"version": 1, "execution_id": executionID, "state": "open"})
	command := func() *exec.Cmd {
		args := []string{"sync", "--config", filepath.Join(runtime, "source.json"), "--destination", filepath.Join(runtime, "destination.json"), "--available-streams", filepath.Join(runtime, "available_streams.json"), "--selected-streams", filepath.Join(runtime, "selected_streams.json"), "--state", filepath.Join(runtime, "state.json"), "--execution-id", executionID, "--execution-launch-id", originalID, "--execution-fence", filepath.Join(runtime, "execution-fence.json"), "--ordinary-recovery-origin", filepath.Join(runtime, "ordinary-recovery-origin.json"), "--ordinary-recovery-result", filepath.Join(runtime, "ordinary-recovery-result.json")}
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
	interruptedSnapshots := make(map[string]int64)
	if interruptedBarriers >= 0 {
		child := command()
		var output bytes.Buffer
		child.Stdout, child.Stderr = &output, &output
		require.NoError(t, child.Start())
		finished := make(chan error, 1)
		go func() { finished <- child.Wait() }()
		select {
		case <-interrupted:
		case err := <-finished:
			t.Fatalf("CLI exited before the requested catalog interruption: %v\n%s", err, output.String())
		case <-ctx.Done():
			_ = child.Process.Kill()
			<-finished
			t.Fatalf("CLI did not reach the requested catalog interruption\n%s", output.String())
		}
		require.NoError(t, child.Process.Kill())
		require.Error(t, <-finished)
		for _, artifact := range []string{"ordinary-recovery-server.json", "execution-result.json", "ordinary-recovery-result.json"} {
			_, err := os.Stat(filepath.Join(runtime, artifact))
			require.True(t, os.IsNotExist(err), "abruptly killed helper must not publish %s", artifact)
		}
		for name, want := range map[string][]byte{"state.json": baseline, "ordinary-recovery-origin.json": originBytes} {
			data, err := os.ReadFile(filepath.Join(runtime, name))
			require.NoError(t, err)
			require.Equal(t, want, data, "interrupted recovery must preserve %s", name)
		}
		for index, name := range []string{"a", "b"} {
			current, err := cat.LoadTable(ctx, table.Identifier{namespace, name})
			require.NoError(t, err)
			interruptedSnapshots[name] = nativeSnapshotID(current)
			if index < interruptedBarriers {
				require.NotEqual(t, nativeSnapshotID(originals[name]), nativeSnapshotID(current))
				var marker ordinaryRecoveryMarker
				require.NoError(t, json.Unmarshal([]byte(current.Properties()[ordinaryRecoveryProperty]), &marker))
				require.Equal(t, origin, marker.Origin)
				require.Equal(t, originalID, marker.LaunchID)
				require.Equal(t, executionID, marker.ExecutionID)
				require.NotEmpty(t, marker.SourceIdentity)
			} else {
				require.Equal(t, nativeSnapshotID(originals[name]), nativeSnapshotID(current))
			}
		}
		var confirmed string
		require.NoError(t, db.QueryRowContext(ctx, "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name=$1", slot).Scan(&confirmed))
		require.Equal(t, lower, confirmed, "partial barriers must not acknowledge source WAL")
		write("execution-fence.json", map[string]any{"version": 1, "execution_id": executionID, "state": "closed"})
		runtime = filepath.Join(root, "successor")
		require.NoError(t, os.MkdirAll(runtime, 0700))
		for name, data := range map[string][]byte{"source.json": sourceBytes, "destination.json": destinationBytes, "available_streams.json": availableBytes, "selected_streams.json": selectedBytes, "state.json": baseline} {
			require.NoError(t, os.WriteFile(filepath.Join(runtime, name), data, 0600))
		}
		executionID = "successor-" + id
		write("ordinary-recovery-origin.json", origin)
		write("execution-fence.json", map[string]any{"version": 1, "execution_id": executionID, "state": "open"})
	}
	output, err := command().CombinedOutput()
	if err != nil {
		t.Fatalf("exported recovery CLI failed: %v\n%s", err, output)
	}
	read := func(name string) []byte {
		data, err := os.ReadFile(filepath.Join(runtime, name))
		require.NoError(t, err)
		return data
	}
	var result types.OrdinaryRecoveryResult
	require.NoError(t, json.Unmarshal(read("ordinary-recovery-result.json"), &result))
	require.Equal(t, origin, result.OrdinaryRecoveryOrigin)
	require.Equal(t, executionID, result.ExecutionID)
	require.Equal(t, originalID, result.LaunchID)
	require.Equal(t, target, result.TargetLSN)
	require.Equal(t, "current_source_lookup", result.ToastValueSemantics)
	require.Len(t, result.Tables, 2)
	for _, proof := range result.Tables {
		name := strings.TrimPrefix(proof.Stream, namespace+".")
		require.NotEqual(t, nativeSnapshotID(originals[name]), proof.FenceSnapshotID)
		if snapshot, ok := interruptedSnapshots[name]; ok {
			require.NotEqual(t, snapshot, proof.FenceSnapshotID, "successor must publish a fresh barrier, not adopt the old one")
		}
	}
	var pin struct {
		OriginalLaunchID string `json:"original_launch_id"`
		ServerIdentity   string `json:"server_identity"`
	}
	require.NoError(t, json.Unmarshal(read("ordinary-recovery-server.json"), &pin))
	require.Equal(t, originalID, pin.OriginalLaunchID)
	require.Equal(t, result.ServerIdentity, pin.ServerIdentity)
	require.Equal(t, originBytes, read("ordinary-recovery-origin.json"))
	var generic utils.ExecutionResult
	require.NoError(t, json.Unmarshal(read("execution-result.json"), &generic))
	require.Equal(t, "settled", generic.Outcome)
	require.Equal(t, executionID, generic.ExecutionID)
	require.Equal(t, originalID, generic.LaunchID)
	finalState := read("state.json")
	finalHash, err := utils.CanonicalStateSHA256(finalState)
	require.NoError(t, err)
	require.Equal(t, finalHash, result.StateSHA256)
	require.Equal(t, finalHash, generic.StateSHA256)
	require.Contains(t, string(finalState), "9007199254740993")
	unchanged, err := os.ReadFile(originalReceiptPath)
	if missingReceipt {
		require.True(t, os.IsNotExist(err), "recovery must never synthesize an original receipt")
		require.Empty(t, result.OriginalReceiptSHA256)
	} else {
		require.NoError(t, err)
		require.Equal(t, oldReceipt, unchanged)
	}
	var confirmed string
	require.NoError(t, db.QueryRowContext(ctx, "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name=$1", slot).Scan(&confirmed))
	require.Equal(t, target, confirmed)
	for _, name := range []string{"a", "b"} {
		current, err := cat.LoadTable(ctx, table.Identifier{namespace, name})
		require.NoError(t, err)
		require.Equal(t, originals[name].Metadata().TableUUID(), current.Metadata().TableUUID())
		require.Equal(t, originals[name].Schema().ID, current.Schema().ID)
		require.True(t, originals[name].Schema().Equals(current.Schema()), "bounded recovery must preserve the frozen destination schema")
		for _, snapshot := range originals[name].Metadata().Snapshots() {
			require.NotNil(t, current.Metadata().SnapshotByID(snapshot.SnapshotID), "old history must survive")
		}
		if !mixed || name == "a" {
			expectedSnapshots := len(originals[name].Metadata().Snapshots()) + 1
			if previous, ok := interruptedSnapshots[name]; ok && previous != nativeSnapshotID(originals[name]) {
				expectedSnapshots++
			}
			require.Len(t, current.Metadata().Snapshots(), expectedSnapshots, "an ahead table receives only fresh barriers, never older replay writes")
		}
		rows := ordinaryRecoverySmokeRows(t, current)
		require.Len(t, rows, 3, "one current row and one tombstone per deleted key, without duplicates or future inserts")
		require.Equal(t, "d", rows[utils.GetKeysHash(map[string]any{"id": int64(0)}, "id")].operation)
		require.Equal(t, "d", rows[utils.GetKeysHash(map[string]any{"id": int64(3)}, "id")].operation)
		want := int64(10)
		if name == "b" {
			want = 20
		}
		row := rows[utils.GetKeysHash(map[string]any{"id": int64(1)}, "id")]
		require.NotEqual(t, "d", row.operation)
		require.True(t, row.hasValue)
		require.Equal(t, want, row.value)
	}
}

type ordinaryRecoverySmokeRow struct {
	operation string
	value     int64
	hasValue  bool
}

func ordinaryRecoverySmokeRows(t *testing.T, tbl *table.Table) map[string]ordinaryRecoverySmokeRow {
	t.Helper()
	scan := tbl.Scan(table.WithSelectedFields(constants.OlakeID, constants.OpType, "value"))
	defer scan.Close()
	schema, records, err := scan.ToArrowRecords(context.Background())
	require.NoError(t, err)
	idColumn, opColumn, valueColumn := schema.FieldIndices(constants.OlakeID)[0], schema.FieldIndices(constants.OpType)[0], schema.FieldIndices("value")[0]
	rows := make(map[string]ordinaryRecoverySmokeRow)
	for batch, err := range records {
		require.NoError(t, err)
		ids, operations, values := batch.Column(idColumn).(*array.String), batch.Column(opColumn).(*array.String), batch.Column(valueColumn).(*array.Int64)
		for index := range int(batch.NumRows()) {
			id := ids.Value(index)
			_, duplicate := rows[id]
			require.False(t, duplicate, "duplicate visible key %s", id)
			rows[id] = ordinaryRecoverySmokeRow{operation: operations.Value(index), value: values.Value(index), hasValue: values.IsValid(index)}
		}
		batch.Release()
	}
	return rows
}

func ordinaryRecoveryCLIRow(id, value int64, lsn string) types.RawRecord {
	return types.RawRecord{Data: map[string]any{"id": id, "value": value}, OlakeColumns: map[string]any{constants.OlakeID: utils.GetKeysHash(map[string]any{"id": id}, "id"), constants.OpType: "r", constants.OlakeTimestamp: time.Now().UTC(), constants.CdcTimestamp: time.Now().UTC(), "_cdc_lsn": lsn}}
}

func ordinaryRecoverySmokeHash(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func ordinaryRecoveryPeekCommit(t *testing.T, ctx context.Context, db *sql.DB, slot, publication string) string {
	t.Helper()
	rows, err := db.QueryContext(ctx, "SELECT data FROM pg_logical_slot_peek_binary_changes($1,NULL,NULL,'proto_version','1','publication_names',$2)", slot, publication)
	require.NoError(t, err)
	defer rows.Close()
	var target uint64
	for rows.Next() {
		var data []byte
		require.NoError(t, rows.Scan(&data))
		if len(data) == 26 && data[0] == 'C' {
			target = binary.BigEndian.Uint64(data[10:18])
		}
	}
	require.NoError(t, rows.Err())
	require.NotZero(t, target, "real pgoutput COMMIT end boundary required")
	return fmt.Sprintf("%X/%X", target>>32, target&0xffffffff)
}

// This test-only HTTP adapter transports genuine catalog load/commit results;
// SQLCatalog validates every real requirement and writes genuine metadata files.
func ordinaryRecoverySQLREST(t *testing.T, cat catalog.Catalog, onCommit func(context.Context, bool)) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		respond := func(value any) {
			if err := json.NewEncoder(w).Encode(value); err != nil {
				t.Errorf("encode SQL REST response: %v", err)
			}
		}
		fail := func(status int, err error) {
			w.WriteHeader(status)
			respond(map[string]any{"error": map[string]any{"message": err.Error(), "type": "SQLCatalogException", "code": status}})
		}
		if r.Method == http.MethodGet && r.URL.Path == "/v1/config" {
			respond(map[string]any{"defaults": map[string]string{}, "overrides": map[string]string{}})
			return
		}
		parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		if len(parts) == 3 && parts[0] == "v1" && parts[1] == "namespaces" {
			if r.Method == http.MethodHead {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			if r.Method == http.MethodGet {
				respond(map[string]any{"namespace": strings.Split(parts[2], "\x1f"), "properties": map[string]string{}})
				return
			}
		}
		if len(parts) != 5 || parts[0] != "v1" || parts[1] != "namespaces" || parts[3] != "tables" {
			fail(404, fmt.Errorf("unsupported fixture REST route %s %s", r.Method, r.URL.Path))
			return
		}
		ident := append(strings.Split(parts[2], "\x1f"), parts[4])
		if r.Method == http.MethodGet {
			tbl, err := cat.LoadTable(r.Context(), ident)
			if err != nil {
				fail(404, err)
				return
			}
			respond(map[string]any{"metadata-location": tbl.MetadataLocation(), "metadata": tbl.Metadata(), "config": map[string]string{}})
			return
		}
		if r.Method == http.MethodPost {
			var request struct {
				Requirements table.Requirements `json:"requirements"`
				Updates      table.Updates      `json:"updates"`
			}
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				fail(400, err)
				return
			}
			onCommit(r.Context(), false)
			if r.Context().Err() != nil {
				return
			}
			metadata, location, err := cat.CommitTable(r.Context(), ident, request.Requirements, request.Updates)
			if err != nil {
				status := 500
				if errors.Is(err, table.ErrCommitFailed) {
					status = 409
				}
				fail(status, err)
				return
			}
			onCommit(r.Context(), true)
			if r.Context().Err() != nil {
				return
			}
			respond(map[string]any{"metadata-location": location, "metadata": metadata})
			return
		}
		fail(405, fmt.Errorf("unsupported fixture REST method %s", r.Method))
	}))
}
