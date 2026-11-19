package types

import (
	"strconv"
	"testing"

	"github.com/goccy/go-json"
	"github.com/stretchr/testify/require"
)

func TestCopyOnWriteWireDefaultsAndValidation(t *testing.T) {
	stream := NewStream("rows", "public", nil).WithPrimaryKey("Source ID").Wrap(0)
	stream.Stream.UpsertField("Source ID", Int64, false, false)
	require.NoError(t, json.Unmarshal([]byte(`{"merge_strategy":"copy-on-write","normalization":true,"append_mode":false,"cow_options":{"max_batch_rows":7}}`), &stream.StreamMetadata))
	require.True(t, stream.CopyOnWriteEnabled())
	options, err := stream.EffectiveCOWOptions()
	require.NoError(t, err)
	require.Equal(t, 7, options.MaxBatchRows)
	require.Equal(t, 300, options.FlushIntervalSeconds)
	require.Equal(t, 67108864, options.MaxBatchBytes)
	require.Equal(t, 2, options.RewriteConcurrency)
	require.False(t, options.MaterializeExistingDeletes)
	require.NoError(t, stream.ValidateCopyOnWrite(Iceberg))
	require.Error(t, stream.ValidateCopyOnWrite(Parquet))
	stream.StreamMetadata.CowOptions.MaxBatchRows = 0
	_, err = stream.EffectiveCOWOptions()
	require.Error(t, err)
	stream.StreamMetadata.CowOptions = nil
	stream.StreamMetadata.MergeStrategy = "unknown"
	require.Error(t, stream.ValidateCopyOnWrite(Iceberg))
}

func TestCopyOnWriteInactiveAppendAndSavedDeletePreference(t *testing.T) {
	stream := NewStream("rows", "public", nil).Wrap(0)
	stream.StreamMetadata = StreamMetadata{AppendMode: new(true), MergeStrategy: CopyOnWrite, UpdateType: new("dv")}
	require.False(t, stream.CopyOnWriteEnabled())
	require.NoError(t, stream.ValidateCopyOnWrite(Parquet), "explicit append preserves an inactive policy")
	mergeUpdateType(&stream.StreamMetadata, stream.ID(), []UpdateType{})
	require.Equal(t, "dv", *stream.StreamMetadata.UpdateType)
	stream.StreamMetadata.AppendMode = new(false)
	require.Error(t, stream.ValidateCopyOnWrite(Iceberg), "missing PK/normalization cannot silently fall back to append")
}

func TestCopyOnWriteRejectsNullableOrUnselectedKey(t *testing.T) {
	stream := NewStream("rows", "public", nil).WithPrimaryKey("id").Wrap(0)
	stream.Stream.UpsertField("id", Int64, true, false)
	stream.StreamMetadata = StreamMetadata{AppendMode: new(false), Normalization: new(true), MergeStrategy: CopyOnWrite}
	require.ErrorContains(t, stream.ValidateCopyOnWrite(Iceberg), "nonnull")
	stream.Stream.Schema = NewTypeSchema()
	stream.Stream.UpsertField("id", Int64, false, false)
	stream.StreamMetadata.SelectedColumns = &SelectedColumns{Columns: []string{"other"}}
	require.ErrorContains(t, stream.ValidateCopyOnWrite(Iceberg), "unselected")
}

func TestCopyOnWriteFlushIntervalDurationBounds(t *testing.T) {
	if strconv.IntSize < 64 {
		t.Skip("32-bit intervals cannot overflow time.Duration seconds")
	}
	stream := NewStream("rows", "public", nil).Wrap(0)
	options := DefaultCOWOptions()
	stream.StreamMetadata.CowOptions = &options
	maximum := MaxCOWFlushIntervalSeconds
	options.FlushIntervalSeconds = int(maximum)
	_, err := stream.EffectiveCOWOptions()
	require.NoError(t, err)
	options.FlushIntervalSeconds = int(maximum + 1)
	_, err = stream.EffectiveCOWOptions()
	require.ErrorContains(t, err, "maximum representable duration")
}
