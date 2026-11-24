// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package metadata_test

import (
	"bytes"
	"errors"
	"fmt"
	"runtime"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/stretchr/testify/require"
)

func newCompactionTestBuilder(t *testing.T, maxBytes uint32, candidates int) (metadata.BloomFilterBuilder, *memory.CheckedAllocator) {
	t.Helper()
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	column := schema.NewColumn(schema.NewByteArrayNode("value", parquet.Repetitions.Required, -1), 1, 1)
	builder := metadata.NewAdaptiveBlockSplitBloomFilter(maxBytes, candidates, 0.01, column, mem)
	t.Cleanup(func() {
		builder.Release()
		mem.AssertSize(t, 0)
	})
	return builder, mem
}

func compactTestBuilder(t *testing.T, builder metadata.BloomFilterBuilder) {
	t.Helper()
	compact, ok := builder.(interface{ Compact() })
	require.True(t, ok, "adaptive builders must support optional compaction")
	compact.Compact()
}

func compactionTestHashes(builder metadata.BloomFilterBuilder, count int) []uint64 {
	hashes := make([]uint64, count)
	for i := range hashes {
		hashes[i] = metadata.GetHash(builder.Hasher(), int64(i))
	}
	return hashes
}

func insertCompactionTestHashes(builder metadata.BloomFilterBuilder, hashes []uint64, bulk, duplicates bool) {
	if !bulk {
		for _, hash := range hashes {
			builder.InsertHash(hash)
			if duplicates {
				builder.InsertHash(hash)
			}
		}
		return
	}

	const batchSize = 97
	for offset := 0; offset < len(hashes); offset += batchSize {
		batch := hashes[offset:min(offset+batchSize, len(hashes))]
		if !duplicates {
			builder.InsertBulk(batch)
			continue
		}

		// Exercise duplicates both within a batch and against previous batches.
		repeated := make([]uint64, 0, 2*len(batch))
		for _, hash := range batch {
			repeated = append(repeated, hash, hash)
		}
		builder.InsertBulk(repeated)
		builder.InsertBulk(repeated)
	}
}

func TestAdaptiveBloomCompactionPreservesSelectedBitset(t *testing.T) {
	// With this bounded configuration, candidate NDV cutoffs include 500 and
	// 1500. Also exercise the largest candidate's capacity boundary and beyond.
	counts := []int{0, 1, 499, 500, 501, 1499, 1500, 1501, 2999, 3000, 3001, 6000}
	modes := []struct {
		name       string
		bulk       bool
		duplicates bool
	}{
		{name: "single"},
		{name: "single_duplicates", duplicates: true},
		{name: "bulk", bulk: true},
		{name: "bulk_duplicates", bulk: true, duplicates: true},
	}
	for _, mode := range modes {
		for _, count := range counts {
			t.Run(fmt.Sprintf("%s/ndv_%d", mode.name, count), func(t *testing.T) {
				baseline, baselineMem := newCompactionTestBuilder(t, 8192, 4)
				compacted, compactedMem := newCompactionTestBuilder(t, 8192, 4)
				hashes := compactionTestHashes(baseline, count)
				insertCompactionTestHashes(baseline, hashes, mode.bulk, mode.duplicates)
				insertCompactionTestHashes(compacted, hashes, mode.bulk, mode.duplicates)
				selectedSize := baseline.Size()
				require.Equal(t, selectedSize, compacted.Size())

				compactTestBuilder(t, compacted)
				require.Equal(t, selectedSize, compacted.Size())
				require.EqualValues(t, selectedSize, compactedMem.CurrentAlloc())
				runtime.GC()
				runtime.GC()

				filter, ok := compacted.(metadata.BloomFilter)
				require.True(t, ok)
				for _, hash := range hashes {
					require.True(t, filter.CheckHash(hash), "false negative after compaction and GC for hash %d", hash)
				}

				var want, got bytes.Buffer
				n, err := baseline.WriteTo(&want, nil)
				require.NoError(t, err)
				require.EqualValues(t, selectedSize, n)
				n, err = compacted.WriteTo(&got, nil)
				require.NoError(t, err)
				require.EqualValues(t, selectedSize, n)
				require.Equal(t, want.Bytes(), got.Bytes(), "compaction must not change the selected bitset")
				require.Zero(t, baselineMem.CurrentAlloc())
				require.Zero(t, compactedMem.CurrentAlloc())
				compactTestBuilder(t, compacted)
			})
		}
	}
}

func TestAdaptiveBloomCompactionReleasesUnusedMemory(t *testing.T) {
	builder, mem := newCompactionTestBuilder(t, 1<<20, 8)
	builder.InsertBulk(compactionTestHashes(builder, 100))
	selectedSize := builder.Size()
	before := mem.CurrentAlloc()
	require.Greater(t, before, int(selectedSize))

	compactTestBuilder(t, builder)
	require.Equal(t, selectedSize, builder.Size())
	require.EqualValues(t, selectedSize, mem.CurrentAlloc(), "only the selected bitset should remain allocated")
	require.Less(t, mem.CurrentAlloc(), before)
	compactTestBuilder(t, builder)
	require.EqualValues(t, selectedSize, mem.CurrentAlloc())

	builder.Release()
	require.Zero(t, mem.CurrentAlloc())
	builder.Release()
	compactTestBuilder(t, builder)
	require.Zero(t, mem.CurrentAlloc())
}

func TestAdaptiveBloomCompactionFinalizesBuilder(t *testing.T) {
	for _, config := range []struct {
		name       string
		maxBytes   uint32
		candidates int
	}{
		{name: "adaptive", maxBytes: 8192, candidates: 4},
		{name: "single_candidate", maxBytes: 8192, candidates: 1},
		{name: "minimum_size", maxBytes: 32, candidates: 4},
		{name: "fallback_candidate", maxBytes: 8192, candidates: 0},
	} {
		t.Run(config.name, func(t *testing.T) {
			builder, mem := newCompactionTestBuilder(t, config.maxBytes, config.candidates)
			hashes := compactionTestHashes(builder, 3)
			builder.InsertBulk(hashes)
			selectedSize := builder.Size()
			compactTestBuilder(t, builder)
			retained := mem.CurrentAlloc()
			compactTestBuilder(t, builder)
			require.Equal(t, retained, mem.CurrentAlloc())
			require.Equal(t, selectedSize, builder.Size())
			require.Panics(t, func() { builder.InsertHash(hashes[0]) })
			require.Panics(t, func() { builder.InsertBulk(hashes) })

			builder.Release()
			builder.Release()
			compactTestBuilder(t, builder)
			require.Zero(t, mem.CurrentAlloc())
		})
	}
}

type compactionFailingWriter struct {
	err     error
	partial bool
}

func (w compactionFailingWriter) Write(p []byte) (int, error) {
	if w.partial {
		return len(p) / 2, w.err
	}
	return 0, w.err
}

func TestAdaptiveBloomCompactionSerializationFailureReleasesMemory(t *testing.T) {
	for _, compact := range []bool{false, true} {
		for _, partial := range []bool{false, true} {
			t.Run(fmt.Sprintf("compact_%t/partial_%t", compact, partial), func(t *testing.T) {
				builder, mem := newCompactionTestBuilder(t, 8192, 4)
				builder.InsertBulk(compactionTestHashes(builder, 100))
				if compact {
					compactTestBuilder(t, builder)
				}
				selectedSize := builder.Size()
				writeErr := errors.New("injected bitset write failure")
				n, err := builder.WriteTo(compactionFailingWriter{err: writeErr, partial: partial}, nil)
				require.ErrorIs(t, err, writeErr)
				if partial {
					require.EqualValues(t, selectedSize/2, n)
				} else {
					require.Zero(t, n)
				}
				require.Zero(t, mem.CurrentAlloc(), "serialization errors must release all candidate buffers")
				compactTestBuilder(t, builder)
				builder.Release()
				builder.Release()
				require.Zero(t, mem.CurrentAlloc())
			})
		}
	}
}
