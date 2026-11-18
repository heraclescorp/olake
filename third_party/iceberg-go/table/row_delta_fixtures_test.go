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
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

func newNoReplayV3Table(t *testing.T) (*table.Table, *concurrentTestCatalog) {
	t.Helper()

	location := filepath.ToSlash(t.TempDir())
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: false},
	)
	meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec, table.UnsortedSortOrder, location,
		iceberg.Properties{
			table.PropertyFormatVersion:        "3",
			table.WriteDeleteModeKey:           table.WriteModeMergeOnRead,
			table.CommitNumRetriesKey:          "2",
			table.CommitMinRetryWaitMsKey:      "1",
			table.CommitMaxRetryWaitMsKey:      "2",
			table.CommitTotalRetryTimeoutMsKey: "1000",
		})
	require.NoError(t, err)

	metaLoc := location + "/metadata/v1.metadata.json"
	fsF := func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }
	cat := &concurrentTestCatalog{metadata: meta, location: metaLoc, fsF: fsF}

	return table.New(table.Identifier{"db", "no_replay_removals"}, meta, metaLoc, fsF, cat), cat
}

type concurrentTestCatalog struct {
	metadata table.Metadata
	location string
	fsF      table.FSysF
	attempts atomic.Int32
}

func (c *concurrentTestCatalog) LoadTable(_ context.Context, ident table.Identifier) (*table.Table, error) {
	return table.New(ident, c.metadata, c.location, c.fsF, c), nil
}

func (c *concurrentTestCatalog) CommitTable(_ context.Context, _ table.Identifier, reqs []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	c.attempts.Add(1)
	for _, req := range reqs {
		if err := req.Validate(c.metadata); err != nil {
			return nil, "", fmt.Errorf("%w: %w", table.ErrCommitFailed, err)
		}
	}
	meta, err := table.UpdateTableMetadata(c.metadata, updates, "")
	if err != nil {
		return nil, "", err
	}
	c.metadata = meta

	// Returning the seed location is enough to keep subsequent
	// NewTransaction calls from adding AssertCreate; the value is not
	// re-read by the table machinery between commits in this stub.
	return meta, c.location, nil
}

func appendTenRows(t *testing.T, tbl *table.Table) *table.Table {
	t.Helper()

	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "data", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	data, err := array.TableFromJSON(memory.DefaultAllocator, arrowSchema, []string{
		`[{"id":1,"data":"a"},{"id":2,"data":"b"},{"id":3,"data":"c"},{"id":4,"data":"d"},` +
			`{"id":5,"data":"e"},{"id":6,"data":"f"},{"id":7,"data":"g"},{"id":8,"data":"h"},` +
			`{"id":9,"data":"i"},{"id":10,"data":"j"}]`,
	})
	require.NoError(t, err)
	defer data.Release()

	tbl, err = tbl.Append(context.Background(), array.NewTableReader(data, -1), nil)
	require.NoError(t, err)

	return tbl
}

func newRewriteDeletionVector(t *testing.T, path, ref string, offset, length *int64) iceberg.DataFile {
	t.Helper()

	builder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentPosDeletes,
		path, iceberg.PuffinFile, nil, nil, nil, 1, 128)
	require.NoError(t, err)
	if ref != "" {
		builder.ReferencedDataFile(ref)
	}
	if offset != nil {
		builder.ContentOffset(*offset)
	}
	if length != nil {
		builder.ContentSizeInBytes(*length)
	}

	return builder.Build()
}

func deleteEntriesReferencing(t *testing.T, tbl *table.Table, refs map[string]struct{}) []string {
	t.Helper()

	snap := tbl.CurrentSnapshot()
	require.NotNil(t, snap)

	fs := iceio.LocalFS{}
	manifests, err := snap.Manifests(fs)
	require.NoError(t, err)

	var found []string
	for _, m := range manifests {
		if m.ManifestContent() != iceberg.ManifestContentDeletes {
			continue
		}
		for e, err := range m.Entries(fs, false) {
			require.NoError(t, err)
			if e.Status() == iceberg.EntryStatusDELETED {
				continue
			}
			ref := e.DataFile().ReferencedDataFile()
			if ref == nil {
				continue
			}
			if _, ok := refs[*ref]; ok {
				found = append(found, e.DataFile().FilePath()+" -> "+*ref)
			}
		}
	}

	return found
}
