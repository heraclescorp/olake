package internal

import (
	"context"
	"io"
	"io/fs"
	"sync"

	iceio "github.com/apache/iceberg-go/io"
)

const (
	parquetReadAheadBytes = 1 << 20
	parquetReadAheadSlots = 4
)

var parquetReadAheadBudget = make(chan struct{}, 128)

type parquetRewriteReadAheadKey struct{}

func WithParquetRewriteReadAhead(ctx context.Context) context.Context {
	return context.WithValue(ctx, parquetRewriteReadAheadKey{}, true)
}

func parquetRewriteReadAhead(ctx context.Context, file iceio.File) iceio.File {
	if ctx.Value(parquetRewriteReadAheadKey{}) != true {
		return file
	}
	remote, ok := file.(interface{ Size() int64 })
	if !ok || remote.Size() <= 0 {
		return file
	}
	return &parquetReadAheadFile{File: file, size: remote.Size(), ctx: ctx}
}

type parquetReadAheadBlock struct {
	mu    sync.Mutex
	start int64
	valid int
	data  []byte
}

type parquetReadAheadFile struct {
	iceio.File
	ctx    context.Context
	size   int64
	mu     sync.RWMutex
	closed bool
	blocks [parquetReadAheadSlots]parquetReadAheadBlock
}

func (f *parquetReadAheadFile) ReadAt(p []byte, off int64) (int, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.closed {
		return 0, fs.ErrClosed
	}
	if err := f.ctx.Err(); err != nil {
		return 0, err
	}
	if len(p) == 0 || off < 0 || off >= f.size {
		return f.File.ReadAt(p, off)
	}
	index := off / parquetReadAheadBytes
	start := index * parquetReadAheadBytes
	within := int(off - start)
	if len(p) > parquetReadAheadBytes-within {
		return f.File.ReadAt(p, off)
	}
	block := &f.blocks[index%parquetReadAheadSlots]
	block.mu.Lock()
	defer block.mu.Unlock()
	if block.data == nil {
		select {
		case parquetReadAheadBudget <- struct{}{}:
			block.data = make([]byte, min(f.size, parquetReadAheadBytes))
		default:
			return f.File.ReadAt(p, off)
		}
	}
	if block.start != start || block.valid == 0 {
		block.valid = 0
		want := int(min(f.size-start, parquetReadAheadBytes))
		n, err := f.File.ReadAt(block.data[:want], start)
		if n != want {
			if err == nil {
				err = io.ErrUnexpectedEOF
			}
			return 0, err
		}
		if err != nil && err != io.EOF {
			return 0, err
		}
		block.start, block.valid = start, n
	}
	n := copy(p, block.data[within:block.valid])
	if n != len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (f *parquetReadAheadFile) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return fs.ErrClosed
	}
	f.closed = true
	for i := range f.blocks {
		if f.blocks[i].data != nil {
			f.blocks[i].data = nil
			<-parquetReadAheadBudget
		}
	}
	return f.File.Close()
}
