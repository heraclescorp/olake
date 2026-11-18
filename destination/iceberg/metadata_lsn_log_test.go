package iceberg

import (
	"context"
	"github.com/apache/iceberg-go/catalog"
	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNativeMetadataLSNPosition(t *testing.T) {
	for _, tc := range []struct{ name, recovery, status, lsn string }{
		{"missing", "", "absent", ""},
		{"malformed", "{", "invalid_recovery", ""},
		{"backfill only", `{"full_refresh_committed_ids":["chunk"]}`, "no_cdc_state", ""},
		{"wrong state type", `{"state":{"lsn":"0/ABC"}}`, "invalid_state_type", ""},
		{"malformed state", `{"state":"{"}`, "invalid_state", ""},
		{"other driver cursor", `{"state":"{\"cursor\":\"sensitive\"}"}`, "no_lsn", ""},
		{"bad LSN", `{"state":"{\"lsn\":\"invalid\"}"}`, "invalid_lsn", ""},
		{"CDC", `{"state":"{\"lsn\":\"0/ABC\"}"}`, "present", "0/ABC"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, lsn := nativeMetadataLSNPosition(tc.recovery)
			require.Equal(t, tc.status, status)
			require.Equal(t, tc.lsn, lsn)
		})
	}
}

// A diagnostic read must not go through writer setup, which can create tables,
// migrate formats, and reconcile a physical index.
func TestMetadataDiagnosticsReadOnly(t *testing.T) {
	ctx := context.Background()
	cfg, stream, cat := nativeWriterFixture(t, types.UpdateTypeEquality, true, "")
	seed := openNativeTestWriter(ctx, t, cfg, stream, cat, nil, "seed-diagnostic", true)
	require.NoError(t, seed.Write(ctx, []types.RawRecord{nativeTestRecord("a", "r", 1, "one")}))
	require.NoError(t, seed.Close(ctx, nil))
	before := seed.table.MetadataLocation()
	adapter := &Iceberg{config: cfg, catalog: cat}
	adapter.LogMetadataPosition(ctx, stream)
	require.Nil(t, adapter.writer)
	fresh, err := cat.LoadTable(ctx, seed.table.Identifier())
	require.NoError(t, err)
	require.Equal(t, before, fresh.MetadataLocation())
	// The same diagnostic on an absent table must not create it.
	stream.StreamMetadata.DestinationTable = "missing_diagnostic_table"
	adapter.LogMetadataPosition(ctx, stream)
	_, err = cat.LoadTable(ctx, catalog.ToIdentifier(stream.GetDestinationDatabase(&cfg.IcebergDatabase), stream.GetDestinationTable()))
	require.ErrorIs(t, err, catalog.ErrNoSuchTable)
}
