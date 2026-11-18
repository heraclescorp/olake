// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package table_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

// Unlike rowDeltaCatalog, this catalog enforces the same CAS requirements as a
// real catalog and can publish another transaction just before the first CAS.
type sameWriteCatalog struct {
	metadata     table.Metadata
	beforeCommit func()
	loads        int
}

func (c *sameWriteCatalog) LoadTable(_ context.Context, id table.Identifier) (*table.Table, error) {
	c.loads++
	return table.New(id, c.metadata, c.metadata.Location()+"/metadata/catalog.metadata.json", func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, c), nil
}

func (c *sameWriteCatalog) CommitTable(_ context.Context, _ table.Identifier, reqs []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	if c.beforeCommit != nil {
		hook := c.beforeCommit
		c.beforeCommit = nil
		hook()
	}
	for _, req := range reqs {
		if err := req.Validate(c.metadata); err != nil {
			return nil, "", fmt.Errorf("%w: %v", table.ErrCommitFailed, err)
		}
	}
	meta, err := table.UpdateTableMetadata(c.metadata, updates, "")
	if err != nil {
		return nil, "", err
	}
	c.metadata = meta
	return meta, meta.Location() + "/metadata/catalog.metadata.json", nil
}

func sameWriteTable(t *testing.T, version int) (*table.Table, *sameWriteCatalog) {
	t.Helper()
	base := newRowDeltaCommitTestTableVersion(t, version)
	metadata, err := table.UpdateTableMetadata(base.Metadata(), []table.Update{table.NewSetPropertiesUpdate(iceberg.Properties{
		table.CommitNumRetriesKey:          "2",
		table.CommitMinRetryWaitMsKey:      "1",
		table.CommitMaxRetryWaitMsKey:      "2",
		table.CommitTotalRetryTimeoutMsKey: "1000",
	})}, "")
	require.NoError(t, err)
	cat := &sameWriteCatalog{metadata: metadata}
	tbl, err := cat.LoadTable(t.Context(), base.Identifier())
	require.NoError(t, err)
	return tbl, cat
}

func sameWriteDelete(t *testing.T, version int, path, target string) iceberg.DataFile {
	t.Helper()
	if version == 3 {
		return buildDVFile(t, path+".puffin", target)
	}
	builder, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec, iceberg.EntryContentPosDeletes,
		path+".parquet", iceberg.ParquetFile, nil, nil, nil, 1, 128)
	require.NoError(t, err)
	return builder.ReferencedDataFile(target).Build()
}

func sameWriteSeed(t *testing.T, tbl *table.Table, file iceberg.DataFile) *table.Table {
	t.Helper()
	tx := tbl.NewTransaction()
	require.NoError(t, tx.NewRowDelta(nil).AddRows(file).Commit(t.Context()))
	result, err := tx.Commit(t.Context())
	require.NoError(t, err)
	return result
}

func TestRowDeltaSameWriteAndCommittedReferences(t *testing.T) {
	for _, version := range []int{2, 3} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			tbl, _ := sameWriteTable(t, version)
			committed := buildDataFile(t, tbl.Location()+"/committed.parquet")
			tbl = sameWriteSeed(t, tbl, committed)
			added := buildDataFile(t, tbl.Location()+"/staged.parquet")
			tx := tbl.NewTransaction()
			require.NoError(t, tx.SetProperties(iceberg.Properties{"olake.recovery": "same-transaction"}))
			newDelete := sameWriteDelete(t, version, tbl.Location()+"/new-delete", added.FilePath())
			require.NoError(t, tx.NewRowDelta(nil).AddRows(added).AddDeletes(
				sameWriteDelete(t, version, tbl.Location()+"/old-delete", committed.FilePath()),
				newDelete,
			).Commit(t.Context()))
			result, err := tx.Commit(t.Context())
			require.NoError(t, err)
			require.Len(t, result.Metadata().Snapshots(), 2, "one snapshot publishes both new data and deletes")
			require.Equal(t, "same-transaction", result.Properties()["olake.recovery"])
			manifests, err := result.CurrentSnapshot().Manifests(iceio.LocalFS{})
			require.NoError(t, err)
			var stagedSequence int64
			var sameWriteSequence int64
			for _, manifest := range manifests {
				for entry, err := range manifest.Entries(iceio.LocalFS{}, true) {
					require.NoError(t, err)
					file := entry.DataFile()
					if file.FilePath() == added.FilePath() {
						stagedSequence = entry.SequenceNum()
					}
					if file.FilePath() == newDelete.FilePath() {
						sameWriteSequence = entry.SequenceNum()
						require.Equal(t, version == 3, table.IsDeletionVector(file))
						if version == 3 {
							require.NotNil(t, file.ReferencedDataFile())
							require.Equal(t, added.FilePath(), *file.ReferencedDataFile())
						}
					}
				}
			}
			require.Positive(t, stagedSequence)
			require.Equal(t, stagedSequence, sameWriteSequence)
		})
	}
}

func TestRowDeltaSameWriteRejectsDuplicateAndCommittedPaths(t *testing.T) {
	for _, version := range []int{2, 3} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			tbl, _ := sameWriteTable(t, version)
			file := buildDataFile(t, tbl.Location()+"/data.parquet")
			del := sameWriteDelete(t, version, tbl.Location()+"/delete", file.FilePath())
			tx := tbl.NewTransaction()
			err := tx.NewRowDelta(nil).AddRows(file, file).AddDeletes(del).Commit(t.Context())
			require.Error(t, err)
			unchanged, err := tx.Commit(t.Context())
			require.NoError(t, err)
			require.Nil(t, unchanged.CurrentSnapshot())
			tbl = sameWriteSeed(t, tbl, file)
			err = tbl.NewTransaction().NewRowDelta(nil).AddRows(file).AddDeletes(del).Commit(t.Context())
			require.ErrorIs(t, err, table.ErrConflictingDataFiles)
		})
	}
}

func TestRowDeltaSameWriteDoesNotExemptMissingCommittedReference(t *testing.T) {
	for _, version := range []int{2, 3} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			tbl, cat := sameWriteTable(t, version)
			tbl = sameWriteSeed(t, tbl, buildDataFile(t, tbl.Location()+"/seed.parquet"))
			base := cat.metadata.CurrentSnapshot().SnapshotID
			added := buildDataFile(t, tbl.Location()+"/new.parquet")
			tx := tbl.NewTransaction()
			require.NoError(t, tx.SetProperties(iceberg.Properties{table.WriteDeleteIsolationLevelKey: "snapshot", "olake.recovery": "must-not-publish"}))
			require.NoError(t, tx.NewRowDelta(nil).AddRows(added).AddDeletes(
				sameWriteDelete(t, version, tbl.Location()+"/new-delete", added.FilePath()),
				sameWriteDelete(t, version, tbl.Location()+"/missing-delete", tbl.Location()+"/missing.parquet"),
			).Commit(t.Context()))
			_, err := tx.Commit(t.Context())
			require.ErrorIs(t, err, table.ErrDataFilesMissing)
			require.Equal(t, base, cat.metadata.CurrentSnapshot().SnapshotID)
			require.NotContains(t, cat.metadata.Properties(), "olake.recovery")
		})
	}
}

func TestRowDeltaSameWriteRechecksCollisionAfterConcurrentCommit(t *testing.T) {
	for _, version := range []int{2, 3} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			tbl, cat := sameWriteTable(t, version)
			tbl = sameWriteSeed(t, tbl, buildDataFile(t, tbl.Location()+"/seed.parquet"))
			added := buildDataFile(t, tbl.Location()+"/collision.parquet")
			tx := tbl.NewTransaction()
			require.NoError(t, tx.NewRowDelta(nil).AddRows(added).AddDeletes(
				sameWriteDelete(t, version, tbl.Location()+"/same-write-delete", added.FilePath()),
			).Commit(t.Context()))
			var concurrentSnapshot int64
			cat.beforeCommit = func() {
				concurrent := sameWriteSeed(t, tbl, added)
				concurrentSnapshot = concurrent.CurrentSnapshot().SnapshotID
			}
			loads := cat.loads
			_, err := tx.Commit(t.Context())
			require.ErrorIs(t, err, table.ErrConflictingDataFiles)
			require.Equal(t, loads+1, cat.loads, "retry must load the fresh head before detecting the collision")
			require.Equal(t, concurrentSnapshot, cat.metadata.CurrentSnapshot().SnapshotID)
		})
	}
}

func TestRowDeltaSameWriteStillRejectsConcurrentRemovalOfCommittedTarget(t *testing.T) {
	for _, version := range []int{2, 3} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			tbl, cat := sameWriteTable(t, version)
			old := buildDataFile(t, tbl.Location()+"/old.parquet")
			tbl = sameWriteSeed(t, tbl, old)
			added := buildDataFile(t, tbl.Location()+"/staged.parquet")
			tx := tbl.NewTransaction()
			require.NoError(t, tx.NewRowDelta(nil).AddRows(added).AddDeletes(
				sameWriteDelete(t, version, tbl.Location()+"/old-delete", old.FilePath()),
				sameWriteDelete(t, version, tbl.Location()+"/staged-delete", added.FilePath()),
			).Commit(t.Context()))
			cat.beforeCommit = func() {
				other := tbl.NewTransaction()
				require.NoError(t, other.NewRewrite(nil).DeleteFile(old).AddDataFile(buildDataFile(t, tbl.Location()+"/rewritten.parquet")).Commit(t.Context()))
				_, err := other.Commit(t.Context())
				require.NoError(t, err)
			}
			loads := cat.loads
			_, err := tx.Commit(t.Context())
			require.ErrorIs(t, err, table.ErrDataFilesMissing)
			require.Equal(t, loads+1, cat.loads, "retry must load the fresh head before validating the removed target")
		})
	}
}

func TestRowDeltaSameWriteRejectsMultipleDeletionVectors(t *testing.T) {
	tbl, _ := sameWriteTable(t, 3)
	file := buildDataFile(t, tbl.Location()+"/new.parquet")
	tx := tbl.NewTransaction()
	err := tx.NewRowDelta(nil).AddRows(file).AddDeletes(
		sameWriteDelete(t, 3, tbl.Location()+"/first", file.FilePath()),
		sameWriteDelete(t, 3, tbl.Location()+"/second", file.FilePath()),
	).Commit(t.Context())
	require.Error(t, err)
	unchanged, err := tx.Commit(t.Context())
	require.NoError(t, err)
	require.Nil(t, unchanged.CurrentSnapshot())
}

func TestRowDeltaSameWriteFreezesStagedBuilderMembership(t *testing.T) {
	tbl, _ := sameWriteTable(t, 3)
	file := buildDataFile(t, tbl.Location()+"/new.parquet")
	tx := tbl.NewTransaction()
	delta := tx.NewRowDelta(nil).AddRows(file).AddDeletes(sameWriteDelete(t, 3, tbl.Location()+"/same-write", file.FilePath()))
	require.NoError(t, delta.Commit(t.Context()))
	delta.AddDeletes(sameWriteDelete(t, 3, tbl.Location()+"/not-staged", tbl.Location()+"/missing.parquet"))
	_, err := tx.Commit(t.Context())
	require.NoError(t, err)
}
