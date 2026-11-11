package destination

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/errs"
)

func TestRejectUnavailableValues(t *testing.T) {
	record := func(data map[string]any, unavailable ...string) types.RawRecord {
		return types.CreateRawRecord(data, map[string]any{constants.OlakeID: "row-1"}, unavailable)
	}

	testCases := []struct {
		name      string
		records   []types.RawRecord
		wantError bool
	}{
		{
			name:      "a marked column still holding the placeholder fails the batch",
			records:   []types.RawRecord{record(map[string]any{"id": 1, "doc": constants.UnavailableValue}, "doc")},
			wantError: true,
		},
		{
			name: "the first unrecovered record fails even after recovered ones",
			records: []types.RawRecord{
				record(map[string]any{"id": 1, "doc": "recovered"}, "doc"),
				record(map[string]any{"id": 2, "doc": constants.UnavailableValue}, "doc"),
			},
			wantError: true,
		},
		{
			name:    "a marked column recovered from the destination passes",
			records: []types.RawRecord{record(map[string]any{"id": 1, "doc": "recovered"}, "doc")},
		},
		{
			name:    "a marked column left out of the stream selection passes",
			records: []types.RawRecord{record(map[string]any{"id": 1}, "doc")},
		},
		{
			name:    "an unmarked column whose source text equals the placeholder passes",
			records: []types.RawRecord{record(map[string]any{"id": 1, "note": constants.UnavailableValue})},
		},
	}

	stream := &types.ConfiguredStream{Stream: types.NewStream("toast_table", "public", nil)}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := RejectUnavailableValues(stream, tc.records)
			if !tc.wantError {
				require.NoError(t, err)
				return
			}

			require.ErrorIs(t, err, constants.ErrNonRetryable)
			failure := errs.From(err)
			assert.Equal(t, errs.DestinationWriteError, failure.Category)
			assert.Equal(t, codeUnavailableValueUnrecoverable, failure.Code)
			assert.Contains(t, err.Error(), `"doc"`)
		})
	}
}
