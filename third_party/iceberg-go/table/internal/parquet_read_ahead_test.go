package internal

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/require"
)

type rangeTestIO struct {
	iceio.IO
	ctx  context.Context
	url  string
	data []byte
}

func (f rangeTestIO) Open(string) (iceio.File, error) {
	return &rangeTestFile{File: nil, reader: bytes.NewReader(f.data), url: f.url, ctx: f.ctx}, nil
}

type rangeTestFile struct {
	iceio.File
	reader *bytes.Reader
	url    string
	ctx    context.Context
}

func (f *rangeTestFile) Size() int64 { return f.reader.Size() }
func (f *rangeTestFile) Seek(offset int64, whence int) (int64, error) {
	return f.reader.Seek(offset, whence)
}
func (f *rangeTestFile) Close() error { return nil }
func (f *rangeTestFile) ReadAt(p []byte, off int64) (int, error) {
	req, err := http.NewRequestWithContext(f.ctx, http.MethodGet, f.url, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", off, off+int64(len(p))-1))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusPartialContent {
		return 0, fmt.Errorf("unexpected range status %d", resp.StatusCode)
	}
	n, err := io.ReadFull(resp.Body, p)
	if err == io.ErrUnexpectedEOF {
		err = io.EOF
	}
	return n, err
}

func TestParquetRewriteReadAheadReducesHTTPRanges(t *testing.T) {
	ctx := t.Context()
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}, {Name: "payload", Type: arrow.BinaryTypes.String}}, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	var encoded bytes.Buffer
	writer, err := pqarrow.NewFileWriter(schema, &encoded, parquet.NewWriterProperties(), pqarrow.DefaultWriterProps())
	require.NoError(t, err)
	const groups, rowsPerGroup = 64, 16
	padding := strings.Repeat("x", 6144)
	for group := range groups {
		for row := range rowsPerGroup {
			value := int64(group*rowsPerGroup + row)
			builder.Field(0).(*array.Int64Builder).Append(value)
			builder.Field(1).(*array.StringBuilder).Append(fmt.Sprintf("row-%d%s", value, padding))
		}
		batch := builder.NewRecordBatch()
		err := writer.Write(batch)
		batch.Release()
		require.NoError(t, err)
	}
	require.NoError(t, writer.Close())
	data := encoded.Bytes()
	require.Greater(t, len(data), parquetReadAheadSlots*parquetReadAheadBytes)
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		requests.Add(1)
		http.ServeContent(w, req, "data.parquet", time.Time{}, bytes.NewReader(data))
	}))
	defer server.Close()
	file, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec, iceberg.EntryContentData, "data.parquet", iceberg.ParquetFile, nil, nil, nil, groups*rowsPerGroup, int64(len(data)))
	require.NoError(t, err)
	source := &ParquetFileSource{fs: rangeTestIO{ctx: ctx, url: server.URL, data: data}, file: file.Build(), mem: memory.DefaultAllocator}
	read := func(ctx context.Context) int64 {
		requests.Store(0)
		rdr, err := source.GetReader(ctx)
		require.NoError(t, err)
		defer rdr.Close()
		records, err := rdr.GetRecords(ctx, nil, nil)
		require.NoError(t, err)
		defer records.Release()
		var seen int64
		for records.Next() {
			batch := records.RecordBatch()
			for row := range int(batch.NumRows()) {
				require.Equal(t, seen, batch.Column(0).(*array.Int64).Value(row))
				require.Equal(t, fmt.Sprintf("row-%d%s", seen, padding), batch.Column(1).(*array.String).Value(row))
				seen++
			}
		}
		require.NoError(t, records.Err())
		require.EqualValues(t, groups*rowsPerGroup, seen)
		return requests.Load()
	}
	baseline := read(ctx)
	buffered := read(WithParquetRewriteReadAhead(ctx))
	t.Logf("HTTP range requests: original=%d read-ahead=%d (Parquet bytes=%d, rows=%d)", baseline, buffered, len(data), groups*rowsPerGroup)
	require.Less(t, buffered*4, baseline)
}
