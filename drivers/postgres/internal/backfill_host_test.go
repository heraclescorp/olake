package driver

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/datazip-inc/olake/drivers/postgres/internal/waljs"
	"github.com/datazip-inc/olake/pkg/jdbc"
	"github.com/datazip-inc/olake/types"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
)

func readerAppliedAt(positions ...int64) (func(context.Context) (int64, error), *int) {
	calls := 0
	return func(context.Context) (int64, error) {
		position := positions[min(calls, len(positions)-1)]
		calls++
		return position, nil
	}, &calls
}

func TestWaitForAppliedLSN(t *testing.T) {
	t.Run("returns at the first applied position reaching the target", func(t *testing.T) {
		readApplied, calls := readerAppliedAt(98, 99, 100, 101)
		applied, _, err := waitForAppliedLSN(context.Background(), readApplied, 100, time.Second, time.Millisecond)
		if err != nil {
			t.Fatalf("expected the reader to be caught up, got: %v", err)
		}
		if applied != 100 || *calls != 3 {
			t.Fatalf("expected to stop at applied=100 after 3 reads, got applied=%d after %d reads", applied, *calls)
		}
	})

	t.Run("never succeeds while the reader stays behind", func(t *testing.T) {
		readApplied, _ := readerAppliedAt(99)
		timeout := 20 * time.Millisecond
		applied, waited, err := waitForAppliedLSN(context.Background(), readApplied, 100, timeout, time.Millisecond)
		if err == nil {
			t.Fatalf("expected an error while the reader is one position behind")
		}
		if applied != 99 || waited < timeout {
			t.Fatalf("expected applied=99 after at least %s, got applied=%d after %s", timeout, applied, waited)
		}
	})

	t.Run("stops when the context is canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		readApplied := func(context.Context) (int64, error) {
			cancel()
			return 1, nil
		}
		_, _, err := waitForAppliedLSN(ctx, readApplied, 100, time.Minute, time.Minute)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got: %v", err)
		}
	})

	t.Run("returns a failed position read without retrying", func(t *testing.T) {
		readFailure := errors.New("aurora_replica_status unavailable")
		calls := 0
		readApplied := func(context.Context) (int64, error) {
			calls++
			return 0, readFailure
		}
		_, _, err := waitForAppliedLSN(context.Background(), readApplied, 100, time.Minute, time.Millisecond)
		if !errors.Is(err, readFailure) || calls != 1 {
			t.Fatalf("expected the read failure after one read, got %v after %d reads", err, calls)
		}
	})
}

func configureBackfillTest(t *testing.T, p *Postgres) {
	t.Helper()
	p.config = &Config{Host: "writer.internal", BackfillHost: "reader.internal", Port: 5432, Database: "app", Username: "olake", MaxThreads: 4}
	require.NoError(t, p.config.Validate())
	p.typeMapping = pgTypeToDataTypes()
}

func backfillTestStream() *types.ConfiguredStream {
	return &types.ConfiguredStream{Stream: types.NewStream("items", "public", nil)}
}

func expectBackfillValidation(writer, reader sqlmock.Sqlmock) {
	reader.ExpectQuery(auroraInstanceQuery).WillReturnRows(sqlmock.NewRows([]string{"instance", "in_recovery"}).AddRow("reader-1", true))
	writer.ExpectQuery(auroraClusterInstancesQuery).WillReturnRows(sqlmock.NewRows([]string{"server_id"}).AddRow("writer-1").AddRow("reader-1"))
	reader.ExpectQuery(auroraReaderAppliedLSNQuery).WillReturnRows(sqlmock.NewRows([]string{"current_read_lsn"}).AddRow(100))
	writer.ExpectQuery(auroraWriterDurableLSNQuery).WillReturnRows(sqlmock.NewRows([]string{"durable_lsn"}).AddRow(100))
}

func TestSetupDoesNotUseConfiguredBackfillHost(t *testing.T) {
	for _, mode := range []string{"discovery", "cdc-only", "enrollment"} {
		t.Run(mode, func(t *testing.T) {
			p, writer := newMockPostgres(t)
			configureBackfillTest(t, p)
			writerDB := p.client
			var opens int
			p.poolOpener = func(_ context.Context, connection *url.URL, _ *tls.Config) (*sqlx.DB, error) {
				opens++
				if connection.Hostname() != p.config.Host {
					return nil, errors.New("backfill host is unavailable")
				}
				return writerDB, nil
			}
			writer.ExpectQuery(jdbc.PostgresServerVersionNum()).WillReturnRows(sqlmock.NewRows([]string{"server_version_num"}).AddRow(150000))
			if mode != "discovery" {
				p.config.UpdateMethod = map[string]any{"replication_slot": "olake_slot", "initial_wait_time": 120}
				writer.ExpectQuery(jdbc.PostgresWalLevelQuery()).WillReturnRows(sqlmock.NewRows([]string{"wal_level"}).AddRow("logical"))
				writer.ExpectQuery(jdbc.PostgresReplicationAttrQuery()).WillReturnRows(boolRow("granted", true))
				if mode == "enrollment" {
					writer.ExpectQuery(`SELECT plugin FROM pg_replication_slots WHERE slot_name = $1 AND database = current_database() AND slot_type = 'logical'`).WithArgs("olake_slot").WillReturnRows(sqlmock.NewRows([]string{"plugin"}).AddRow("wal2json"))
				} else {
					writer.ExpectQuery(slotExistsQuery).WithArgs("olake_slot").WillReturnRows(boolRow("exists", true))
					writer.ExpectQuery(fmt.Sprintf(waljs.ReplicationSlotTempl, "olake_slot")).WillReturnRows(sqlmock.NewRows([]string{"plugin", "slot_type", "confirmed_flush_lsn", "current_lsn"}).AddRow("wal2json", "logical", "0/16B3748", "0/16B3780"))
				}
				writer.ExpectQuery(jdbc.PostgresWalSenderTimeoutQuery()).WillReturnRows(sqlmock.NewRows([]string{"timeout"}).AddRow(0))
			}
			if mode == "enrollment" {
				require.NoError(t, p.SetupTableEnrollment(context.Background()))
				require.NoError(t, p.enrollmentIssue)
			} else {
				require.NoError(t, p.Setup(context.Background()))
			}
			writer.ExpectQuery(getPrivilegedTablesTmpl).WillReturnRows(sqlmock.NewRows([]string{"table_schema", "table_name"}).AddRow("public", "items"))
			streams, err := p.GetStreamNames(context.Background())
			require.NoError(t, err)
			require.Len(t, streams, 1)
			if mode == "cdc-only" {
				stream := backfillTestStream()
				p.state = &types.State{RWMutex: &sync.RWMutex{}, Global: &types.GlobalState{State: waljs.WALState{LSN: "0/16B3748"}, Streams: types.NewSet(stream.ID())}}
				writer.ExpectQuery(fmt.Sprintf(waljs.ReplicationSlotTempl, "olake_slot")).WillReturnRows(sqlmock.NewRows([]string{"plugin", "slot_type", "confirmed_flush_lsn", "current_lsn"}).AddRow("wal2json", "logical", "0/16B3748", "0/16B3780"))
				require.NoError(t, p.PreCDC(context.Background(), []types.StreamInterface{stream}))
				cdc, err := p.prepareWALJSConfig(stream)
				require.NoError(t, err)
				require.Equal(t, "writer.internal", cdc.Connection.Hostname())
			}
			require.Equal(t, 1, opens)
			require.Nil(t, p.backfillClient)
			require.Nil(t, p.backfillBarrier)
		})
	}
}

func TestBackfillHostFirstUseValidatesBeforePlanningAndScanning(t *testing.T) {
	p, writer := newMockPostgres(t)
	reader, readerMock := newMockPostgres(t)
	configureBackfillTest(t, p)
	var opens int
	p.poolOpener = func(_ context.Context, connection *url.URL, _ *tls.Config) (*sqlx.DB, error) {
		opens++
		require.Equal(t, "reader.internal", connection.Hostname())
		return reader.client, nil
	}
	expectBackfillValidation(writer, readerMock)
	writer.ExpectQuery(auroraWriterDurableLSNQuery).WillReturnRows(sqlmock.NewRows([]string{"durable_lsn"}).AddRow(101))
	readerMock.ExpectQuery(auroraReaderAppliedLSNQuery).WillReturnRows(sqlmock.NewRows([]string{"current_read_lsn"}).AddRow(101))
	stream := backfillTestStream()
	planningFailure := errors.New("planning row count unavailable")
	readerMock.ExpectQuery(jdbc.PostgresRowCountQuery(stream)).WillReturnError(planningFailure)
	_, err := p.GetOrSplitChunks(context.Background(), nil, stream)
	require.ErrorIs(t, err, planningFailure)
	require.Same(t, reader.client, p.backfillClient)
	require.NotNil(t, p.backfillBarrier)

	readerMock.ExpectQuery(auroraReaderAppliedLSNQuery).WillReturnRows(sqlmock.NewRows([]string{"current_read_lsn"}).AddRow(101))
	readerMock.ExpectBegin()
	chunk := types.Chunk{Min: "'(0,0)'", Max: "'(1,0)'"}
	readerMock.ExpectQuery(jdbc.PostgresChunkScanQuery(stream, "ctid", chunk, "")).WillReturnRows(sqlmock.NewRowsWithColumnDefinition(sqlmock.NewColumn("id").OfType("INT4", int64(0))).AddRow(int64(7)))
	readerMock.ExpectRollback()
	var records []map[string]any
	err = p.ChunkIterator(context.Background(), stream, chunk, func(_ context.Context, record map[string]any, _ int64) error {
		records = append(records, record)
		return nil
	})
	require.NoError(t, err)
	require.Len(t, records, 1)
	require.EqualValues(t, 7, records[0]["id"])
	require.Equal(t, 1, opens)
	require.Zero(t, reader.client.Stats().InUse)
}

func TestConcurrentBackfillFirstUseSharesValidatedPool(t *testing.T) {
	p, writer := newMockPostgres(t)
	reader, readerMock := newMockPostgres(t)
	configureBackfillTest(t, p)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	opened, release := make(chan struct{}), make(chan struct{})
	var opens atomic.Int32
	p.poolOpener = func(ctx context.Context, _ *url.URL, _ *tls.Config) (*sqlx.DB, error) {
		if opens.Add(1) == 1 {
			close(opened)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-release:
			return reader.client, nil
		}
	}
	expectBackfillValidation(writer, readerMock)
	writer.ExpectQuery(auroraWriterDurableLSNQuery).WillReturnRows(sqlmock.NewRows([]string{"durable_lsn"}).AddRow(100))
	const callers = 12
	for range callers {
		readerMock.ExpectQuery(auroraReaderAppliedLSNQuery).WillReturnRows(sqlmock.NewRows([]string{"current_read_lsn"}).AddRow(100))
	}
	results := make(chan error, callers)
	for range callers {
		go func() {
			conn, err := p.backfillConn(ctx, backfillTestStream())
			if conn != nil {
				closeBackfillConn(conn)
			}
			results <- err
		}()
	}
	select {
	case <-opened:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	p.backfillMu.Lock()
	unpublished := p.backfillClient == nil && p.backfillBarrier == nil
	p.backfillMu.Unlock()
	require.True(t, unpublished)
	waiterCtx, cancelWaiter := context.WithCancel(ctx)
	waiter := make(chan error, 1)
	go func() { waiter <- p.ensureBackfillHost(waiterCtx) }()
	cancelWaiter()
	select {
	case err := <-waiter:
		require.ErrorIs(t, err, context.Canceled)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	close(release)
	for range callers {
		select {
		case err := <-results:
			require.NoError(t, err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	require.EqualValues(t, 1, opens.Load())
	require.Zero(t, reader.client.Stats().InUse)
}

func TestBackfillInitializationFailureClosesPoolAndRetries(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		t.Run(fmt.Sprintf("canceled=%t", canceled), func(t *testing.T) {
			p, writer := newMockPostgres(t)
			failed, failedMock := newMockPostgres(t)
			reader, readerMock := newMockPostgres(t)
			configureBackfillTest(t, p)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			opens := 0
			p.poolOpener = func(_ context.Context, _ *url.URL, _ *tls.Config) (*sqlx.DB, error) {
				opens++
				if opens == 1 {
					if canceled {
						cancel()
					}
					return failed.client, nil
				}
				return reader.client, nil
			}
			if !canceled {
				failedMock.ExpectQuery(auroraInstanceQuery).WillReturnRows(sqlmock.NewRows([]string{"instance", "in_recovery"}).AddRow("writer-1", false))
			}
			failedMock.ExpectClose()
			conn, err := p.backfillConn(ctx, backfillTestStream())
			require.Error(t, err)
			require.Nil(t, conn)
			if canceled {
				require.ErrorIs(t, err, context.Canceled)
			}
			require.Zero(t, failed.client.Stats().OpenConnections)
			require.Nil(t, p.backfillClient)
			require.Nil(t, p.backfillBarrier)
			require.Nil(t, p.backfillInit)
			expectBackfillValidation(writer, readerMock)
			writer.ExpectQuery(auroraWriterDurableLSNQuery).WillReturnRows(sqlmock.NewRows([]string{"durable_lsn"}).AddRow(100))
			readerMock.ExpectQuery(auroraReaderAppliedLSNQuery).WillReturnRows(sqlmock.NewRows([]string{"current_read_lsn"}).AddRow(100))
			conn, err = p.backfillConn(context.Background(), backfillTestStream())
			require.NoError(t, err)
			closeBackfillConn(conn)
			require.Equal(t, 2, opens)
			require.Same(t, reader.client, p.backfillClient)
		})
	}
}

func TestBackfillReaderFailureDoesNotFallBackToWriter(t *testing.T) {
	p, _ := newMockPostgres(t)
	configureBackfillTest(t, p)
	unreachable := errors.New("reader unreachable")
	var opens int
	p.poolOpener = func(context.Context, *url.URL, *tls.Config) (*sqlx.DB, error) {
		opens++
		return nil, unreachable
	}
	_, err := p.GetOrSplitChunks(context.Background(), nil, backfillTestStream())
	require.ErrorIs(t, err, unreachable)
	err = p.ChunkIterator(context.Background(), backfillTestStream(), types.Chunk{}, nil)
	require.ErrorIs(t, err, unreachable)
	require.Equal(t, 2, opens)
	require.Nil(t, p.backfillClient)
}

func TestExplicitConnectionCheckValidatesConfiguredBackfillHost(t *testing.T) {
	cases := []struct {
		name       string
		instance   string
		recovery   bool
		applied    any
		durable    any
		connectErr error
		want       string
	}{
		{name: "reader", instance: "reader-1", recovery: true, applied: 100, durable: 100},
		{name: "writer endpoint", instance: "writer-1", want: "which is the writer"},
		{name: "different cluster", instance: "other-reader", recovery: true, want: "not in the writer's cluster"},
		{name: "missing reader LSN", instance: "reader-1", recovery: true, want: "no current_read_lsn"},
		{name: "missing writer LSN", instance: "reader-1", recovery: true, applied: 100, want: "no durable_lsn"},
		{name: "unreachable reader", connectErr: errors.New("reader unreachable"), want: "reader unreachable"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, writer := newMockPostgres(t)
			reader, readerMock := newMockPostgres(t)
			configureBackfillTest(t, p)
			writerDB := p.client
			var readerOpens int
			p.poolOpener = func(_ context.Context, connection *url.URL, _ *tls.Config) (*sqlx.DB, error) {
				if connection.Hostname() == p.config.Host {
					return writerDB, nil
				}
				readerOpens++
				if tc.connectErr != nil {
					return nil, tc.connectErr
				}
				return reader.client, nil
			}
			writer.ExpectQuery(jdbc.PostgresServerVersionNum()).WillReturnRows(sqlmock.NewRows([]string{"server_version_num"}).AddRow(150000))
			require.NoError(t, p.Setup(context.Background()))
			require.Zero(t, readerOpens)
			if tc.connectErr == nil {
				readerMock.ExpectQuery(auroraInstanceQuery).WillReturnRows(sqlmock.NewRows([]string{"instance", "in_recovery"}).AddRow(tc.instance, tc.recovery))
				if tc.recovery {
					writer.ExpectQuery(auroraClusterInstancesQuery).WillReturnRows(sqlmock.NewRows([]string{"server_id"}).AddRow("writer-1").AddRow("reader-1"))
					if tc.instance == "reader-1" {
						readerMock.ExpectQuery(auroraReaderAppliedLSNQuery).WillReturnRows(sqlmock.NewRows([]string{"current_read_lsn"}).AddRow(tc.applied))
						if tc.applied != nil {
							writer.ExpectQuery(auroraWriterDurableLSNQuery).WillReturnRows(sqlmock.NewRows([]string{"durable_lsn"}).AddRow(tc.durable))
						}
					}
				}
				if tc.want != "" {
					readerMock.ExpectClose()
				}
			}
			err := p.CheckConnection(context.Background())
			if tc.want == "" {
				require.NoError(t, err)
				require.Same(t, reader.client, p.backfillClient)
			} else {
				require.Error(t, err)
				require.True(t, strings.Contains(err.Error(), tc.want), "%v", err)
				require.Nil(t, p.backfillClient)
				require.Nil(t, p.backfillBarrier)
				if tc.connectErr == nil {
					require.Zero(t, reader.client.Stats().OpenConnections)
				}
			}
			require.Equal(t, 1, readerOpens)
		})
	}
}

func TestBackfillWithoutReaderUsesWriter(t *testing.T) {
	p, writer := newMockPostgres(t)
	p.backfillClient = p.client
	p.poolOpener = func(context.Context, *url.URL, *tls.Config) (*sqlx.DB, error) {
		t.Fatal("no reader should be opened")
		return nil, errors.New("unexpected pool open")
	}
	require.NoError(t, p.CheckConnection(context.Background()))
	conn, err := p.backfillConn(context.Background(), backfillTestStream())
	require.NoError(t, err)
	closeBackfillConn(conn)
	planningFailure := errors.New("writer planning")
	writer.ExpectQuery(jdbc.PostgresRowCountQuery(backfillTestStream())).WillReturnError(planningFailure)
	_, err = p.GetOrSplitChunks(context.Background(), nil, backfillTestStream())
	require.ErrorIs(t, err, planningFailure)
}

func TestClosingDuringBackfillInitializationDoesNotPublishOrLeak(t *testing.T) {
	p, writer := newMockPostgres(t)
	reader, readerMock := newMockPostgres(t)
	configureBackfillTest(t, p)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	opened := make(chan struct{})
	p.poolOpener = func(ctx context.Context, _ *url.URL, _ *tls.Config) (*sqlx.DB, error) {
		close(opened)
		<-ctx.Done()
		return reader.client, nil
	}
	writer.ExpectClose()
	readerMock.ExpectClose()
	result := make(chan error, 1)
	go func() { result <- p.CheckConnection(ctx) }()
	select {
	case <-opened:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	p.CloseConnection()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	require.Nil(t, p.backfillClient)
	require.Nil(t, p.backfillBarrier)
	require.Zero(t, reader.client.Stats().OpenConnections)
}
