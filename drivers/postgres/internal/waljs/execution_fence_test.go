package waljs

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/datazip-inc/olake/utils"
	"github.com/jackc/pglogrepl"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
)

func TestExecutionFenceRejectsDurableAcknowledgement(t *testing.T) {
	for _, fence := range []string{"missing", `{"version":1,"execution_id":"runtime","state":"closed"}`, `{"version":1,"execution_id":"other","state":"open"}`, `{`} {
		t.Run(fence, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "fence.json")
			if fence != "missing" {
				require.NoError(t, os.WriteFile(path, []byte(fence), 0600))
			}
			ctx := utils.WithExecution(context.Background(), &utils.ExecutionOptions{ID: "runtime", Fence: path})
			// Nil database/connection deliberately proves the fence rejects before any I/O.
			_, err := AdvanceLSN(ctx, nil, "slot", "0/300")
			require.Error(t, err)
			require.Error(t, AcknowledgeLSN(ctx, nil, &Socket{ClientXLogPos: pglogrepl.LSN(0x300)}, false))
			_, err = ConnectReplication(ctx, nil)
			require.Error(t, err)
		})
	}
}

func TestAdvanceLSNRechecksFenceAfterClampedAttempt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fence.json")
	require.NoError(t, os.WriteFile(path, []byte(`{"version":1,"execution_id":"runtime","state":"open"}`), 0600))
	ctx := utils.WithExecution(context.Background(), &utils.ExecutionOptions{ID: "runtime", Fence: path})
	queries := 0
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherFunc(func(expected, actual string) error {
		require.Equal(t, expected, actual)
		queries++
		// Close synchronously during the first SQL call; the clamped retry must not run.
		return os.WriteFile(path, []byte(`{"version":1,"execution_id":"runtime","state":"closed"}`), 0600)
	})))
	require.NoError(t, err)
	defer db.Close()
	mock.ExpectQuery("SELECT end_lsn FROM pg_replication_slot_advance('slot', '0/300')").WillReturnRows(sqlmock.NewRows([]string{"end_lsn"}).AddRow("0/200"))
	_, err = AdvanceLSN(ctx, sqlx.NewDb(db, "sqlmock"), "slot", "0/300")
	require.Error(t, err)
	require.Equal(t, 1, queries)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestOrdinaryRetirementTracksAcknowledgedAndAmbiguousSlotAdvance(t *testing.T) {
	for _, ambiguous := range []bool{false, true} {
		t.Run(map[bool]string{false: "acknowledged", true: "response-lost"}[ambiguous], func(t *testing.T) {
			fence := filepath.Join(t.TempDir(), "execution-fence.json")
			require.NoError(t, os.WriteFile(fence, []byte(`{"version":1,"execution_id":"runtime","state":"open"}`), 0600))
			ctx := utils.WithExecution(context.Background(), &utils.ExecutionOptions{ID: "runtime", Fence: fence})
			utils.BeginOrdinaryExecution(ctx, "sync")
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			expected := mock.ExpectQuery("SELECT end_lsn FROM pg_replication_slot_advance")
			if ambiguous {
				expected.WillReturnError(errors.New("slot advance response lost"))
			} else {
				expected.WillReturnRows(sqlmock.NewRows([]string{"end_lsn"}).AddRow("0/300"))
			}
			_, err = AdvanceLSN(ctx, sqlx.NewDb(db, "sqlmock"), "slot", "0/300")
			if ambiguous {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			utils.SourceDrained(ctx, nil)
			proof, ok := utils.OrdinaryExecutionResult(ctx)
			require.True(t, ok)
			if ambiguous {
				require.Equal(t, "unknown", proof.Outcome)
			} else {
				require.Equal(t, "settled", proof.Outcome)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
