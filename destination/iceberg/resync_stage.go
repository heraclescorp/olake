package iceberg

import (
	"context"
	"errors"
	"io"
	"sync"

	"github.com/apache/iceberg-go/catalog"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
)

// Admission and release are catalog-lifetime scoped. A failed stage session is
// terminal: publication must never mistake a failed/aborted writer for a closed
// successful target writer. Other CDC tables stay on the ordinary catalog.
func routeNativeResyncWriter(ctx context.Context, cat catalog.Catalog, ident table.Identifier) (catalog.Catalog, func(bool), error) {
	if stage, ok := cat.(*nativeResyncCatalog); ok {
		cat = stage.Catalog
	}
	state := nativeTableEntry(cat, ident)
	state.mu.Lock()
	attempt := state.resync
	if attempt == nil {
		state.sessions++
		state.mu.Unlock()
		var once sync.Once
		return cat, func(bool) { once.Do(func() { state.mu.Lock(); state.sessions--; state.mu.Unlock() }) }, nil
	}
	state.mu.Unlock()
	attempt.mu.Lock()
	if attempt.published {
		attempt.mu.Unlock()
		state.mu.Lock()
		state.sessions++
		state.mu.Unlock()
		var once sync.Once
		return cat, func(bool) { once.Do(func() { state.mu.Lock(); state.sessions--; state.mu.Unlock() }) }, nil
	}
	defer attempt.mu.Unlock()
	if err := attempt.active(ctx); err != nil {
		return nil, nil, err
	}
	attempt.sessions++
	var once sync.Once
	return attempt.stage, func(success bool) {
		once.Do(func() {
			attempt.mu.Lock()
			attempt.sessions--
			attempt.failed = attempt.failed || !success
			attempt.mu.Unlock()
		})
	}, nil
}

// A stage cannot remove any original object. Keep private uploads until catalog
// outcome is known, and check the live execution fence on every actual write.
type nativeResyncIO struct {
	iceio.IO
	attempt *resyncControl
	ctx     context.Context
}

func (s *nativeResyncIO) Create(path string) (iceio.FileWriter, error) {
	r := s.attempt
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.active(s.ctx); err != nil {
		return nil, err
	}
	writable, ok := s.IO.(iceio.WriteFileIO)
	if !ok {
		return nil, errors.New("native resync table FileIO does not support writing")
	}
	writer, err := writable.Create(path)
	if err != nil {
		return nil, err
	}
	if r.owned == nil {
		r.owned = make(map[string]struct{})
	}
	r.owned[path] = struct{}{}
	return &nativeResyncFileWriter{FileWriter: writer, attempt: r, ctx: s.ctx}, nil
}

func (s *nativeResyncIO) WriteFile(path string, data []byte) error {
	writer, err := s.Create(path)
	if err != nil {
		return err
	}
	_, err = writer.Write(data)
	return errors.Join(err, writer.Close())
}

func (s *nativeResyncIO) Remove(path string) error {
	r := s.attempt
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.unknown || r.published {
		return errors.New("cannot remove files after a possibly successful resync publication")
	}
	if _, owned := r.owned[path]; !owned {
		return errors.New("cannot remove a file not created by this resync stage")
	}
	return s.IO.Remove(path)
}

type nativeResyncFileWriter struct {
	iceio.FileWriter
	attempt *resyncControl
	ctx     context.Context
}

func (w *nativeResyncFileWriter) Write(data []byte) (int, error) {
	w.attempt.mu.Lock()
	defer w.attempt.mu.Unlock()
	if err := w.attempt.active(w.ctx); err != nil {
		return 0, err
	}
	return w.FileWriter.Write(data)
}

func (w *nativeResyncFileWriter) ReadFrom(reader io.Reader) (int64, error) {
	return io.Copy(struct{ io.Writer }{w}, reader)
}

func (w *nativeResyncFileWriter) Close() error {
	w.attempt.mu.Lock()
	defer w.attempt.mu.Unlock()
	// Close can finalize a multipart upload. Under a revoked fence do not
	// dispatch it; retain the unfinalized upload rather than publish an object.
	if err := w.attempt.active(w.ctx); err != nil {
		w.attempt.failed = true
		return err
	}
	return w.FileWriter.Close()
}
