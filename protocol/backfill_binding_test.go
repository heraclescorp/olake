package protocol

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	s3util "github.com/datazip-inc/olake/utils/s3"
	"github.com/stretchr/testify/require"
)

func backfillJournalFixture(t *testing.T) (*types.BackfillBindingJournal, *types.BackfillRecoveryCheckpoint, string, string, context.Context) {
	t.Helper()
	t.Setenv(constants.EnvS3ConfigFolder, "")
	t.Setenv(constants.EnvStorageMode, "nfs")
	oldBucket, oldPrefix := s3util.JobBucket, s3util.JobPrefix
	s3util.JobBucket, s3util.JobPrefix = "", ""
	t.Cleanup(func() { s3util.JobBucket, s3util.JobPrefix = oldBucket, oldPrefix })
	dir := t.TempDir()
	fence := filepath.Join(dir, "execution-fence.json")
	require.NoError(t, os.WriteFile(fence, []byte(`{"version":1,"execution_id":"fresh-runtime","state":"open"}`), 0600))
	ctx := utils.WithExecution(context.Background(), &utils.ExecutionOptions{ID: "fresh-runtime", LaunchID: "original", Fence: fence, Command: "sync"})
	checkpoint := &types.BackfillRecoveryCheckpoint{Version: 1, ExecutionID: "original", LaunchID: "original", SourceSHA256: strings.Repeat("a", 64), DestinationSHA256: strings.Repeat("b", 64), CatalogSHA256: strings.Repeat("c", 64), ServerIdentity: "live-source", SlotName: "slot", State: json.RawMessage(`{"type":"GLOBAL","global":{"state":{"lsn":"0/100"},"streams":[]}}`)}
	stream := "public.target"
	return newBackfillBindingJournal(ctx, checkpoint), checkpoint, filepath.Join(dir, backfillBindingName(stream)), fence, ctx
}

func TestBackfillBindingJournalCreateOnlyAndInheritedBytes(t *testing.T) {
	journal, checkpoint, output, fence, ctx := backfillJournalFixture(t)
	stream := "public.target"
	observed, err := journal.Read(stream)
	require.NoError(t, err)
	require.Nil(t, observed)
	entry := &types.BackfillBindingJournalEntry{TableUUID: "00000000-0000-0000-0000-000000000001", ShapeSHA256: strings.Repeat("a", 64), Plan: json.RawMessage(`[{"min":null,"max":9007199254740993},{"min":9007199254740993,"max":null}]`)}
	require.NoError(t, journal.Create(stream, entry))
	original, err := os.ReadFile(output)
	require.NoError(t, err)
	var envelope types.BackfillRecoveryBinding
	require.NoError(t, strictRecoveryJSON(original, &envelope))
	require.Equal(t, "original", envelope.OriginalExecutionID)
	require.Equal(t, "original", envelope.OriginalLaunchID)
	require.Equal(t, "0/100", envelope.LowerLSN)
	require.Contains(t, string(envelope.Plan), "9007199254740993")
	require.NotContains(t, string(original), "fresh-runtime")
	observed, err = journal.Read(stream)
	require.NoError(t, err)
	require.True(t, equalBackfillBindingEntries(entry, observed))
	orderedDifferently := *entry
	orderedDifferently.Plan = json.RawMessage(`[{"max":null,"min":9007199254740993},{"max":9007199254740993,"min":null}]`)
	require.NoError(t, journal.Create(stream, &orderedDifferently))
	after, err := os.ReadFile(output)
	require.NoError(t, err)
	require.Equal(t, original, after)
	changed := *entry
	changed.TableUUID = "00000000-0000-0000-0000-000000000002"
	require.Error(t, journal.Create(stream, &changed))
	changed = *entry
	changed.Plan = json.RawMessage(`[{"min":null,"max":null}]`)
	require.Error(t, journal.Create(stream, &changed))
	foreign := *checkpoint
	foreign.ServerIdentity = "foreign-source"
	_, err = newBackfillBindingJournal(ctx, &foreign).Read(stream)
	require.Error(t, err)
	require.NoError(t, os.WriteFile(fence, []byte(`{"version":1,"execution_id":"fresh-runtime","state":"closed"}`), 0600))
	require.Error(t, journal.Create(stream, entry))
	after, err = os.ReadFile(output)
	require.NoError(t, err)
	require.Equal(t, original, after)
}

func TestBackfillBindingJournalRefusesCorruptEvidenceAndPersistenceFailure(t *testing.T) {
	journal, _, output, _, _ := backfillJournalFixture(t)
	entry := &types.BackfillBindingJournalEntry{TableUUID: "00000000-0000-0000-0000-000000000001", ShapeSHA256: strings.Repeat("a", 64), Plan: json.RawMessage(`[{"min":null,"max":null}]`)}
	require.NoError(t, os.WriteFile(output, []byte(`{}`), 0600))
	_, err := journal.Read("public.target")
	require.Error(t, err)
	require.Error(t, journal.Create("public.target", entry))
	retained, err := os.ReadFile(output)
	require.NoError(t, err)
	require.Equal(t, "{}", string(retained))
	require.NoError(t, os.Remove(output))
	require.NoError(t, os.Mkdir(output, 0700))
	require.Error(t, journal.Create("public.target", entry))
	info, err := os.Stat(output)
	require.NoError(t, err)
	require.True(t, info.IsDir())
}
