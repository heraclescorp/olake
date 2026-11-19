package types

import (
	"testing"

	"github.com/goccy/go-json"
	"github.com/stretchr/testify/require"
)

func TestCopyOnWriteWireDefaultsAndValidation(t *testing.T) {
	stream := NewStream("rows", "public", nil).WithPrimaryKey("Source ID").Wrap(0)
	stream.Stream.UpsertField("Source ID", Int64, false, false)
	require.NoError(t, json.Unmarshal([]byte(`{"merge_strategy":"copy-on-write","normalization":true,"append_mode":false}`), &stream.StreamMetadata))
	require.True(t, stream.CopyOnWriteEnabled())
	require.NoError(t, stream.ValidateCopyOnWrite(Iceberg))
	require.Error(t, stream.ValidateCopyOnWrite(Parquet))
	encoded, err := json.Marshal(stream.StreamMetadata)
	require.NoError(t, err)
	var canonical StreamMetadata
	require.NoError(t, json.Unmarshal(encoded, &canonical))
	require.Equal(t, CopyOnWrite, canonical.MergeStrategy)
	legacy := toLegacyStreamMetadata(canonical, stream.Stream)
	encoded, err = json.Marshal(legacy)
	require.NoError(t, err)
	var saved LegacyStreamMetadata
	require.NoError(t, json.Unmarshal(encoded, &saved))
	require.Equal(t, CopyOnWrite, legacyToStreamMetadata(saved).MergeStrategy)
	stream.StreamMetadata.MergeStrategy = "unknown"
	require.Error(t, stream.ValidateCopyOnWrite(Iceberg))
	stream.StreamMetadata.MergeStrategy = ""
	require.False(t, stream.CopyOnWriteEnabled(), "saved/CLI configurations without merge_strategy remain MOR")
	require.NoError(t, stream.ValidateCopyOnWrite(Iceberg))
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

func TestCopyOnWriteDoesNotRequirePhysicalSourceKeyColumns(t *testing.T) {
	stream := NewStream("rows", "public", nil).WithPrimaryKey("id").Wrap(0)
	stream.Stream.UpsertField("id", Int64, false, false)
	stream.StreamMetadata = StreamMetadata{AppendMode: new(false), Normalization: new(true), MergeStrategy: CopyOnWrite,
		SelectedColumns: &SelectedColumns{Columns: []string{"body"}},
	}
	require.NoError(t, stream.ValidateCopyOnWrite(Iceberg), "COW matches the existing canonical _olake_id, not physical source keys")
}
