package iceberg

import (
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
