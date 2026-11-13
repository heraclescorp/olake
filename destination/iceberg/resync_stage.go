package iceberg

import (
	"context"
	"errors"
	"io"
	"sync"

	"github.com/apache/iceberg-go/catalog"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/datazip-inc/olake/utils"
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
	if !attempt.published {
		if err := attempt.activeLocal(ctx); err != nil {
			attempt.mu.Unlock()
			return nil, nil, err
		}
		attempt.mu.Unlock()
		if err := utils.CheckExecutionFence(ctx); err != nil {
			return nil, nil, err
		}
		attempt.mu.Lock()
	}
	if attempt.published {
		attempt.mu.Unlock()
		state.mu.Lock()
		state.sessions++
		state.mu.Unlock()
		var once sync.Once
		return cat, func(bool) { once.Do(func() { state.mu.Lock(); state.sessions--; state.mu.Unlock() }) }, nil
	}
	defer attempt.mu.Unlock()
	if err := attempt.activeLocal(ctx); err != nil {
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

// A stage cannot remove any original object. Private I/O holds a short-lived
// session lease, not the attempt mutex, so publication cannot race in-flight I/O.
type nativeResyncIO struct {
	iceio.IO
	attempt *resyncControl
	ctx     context.Context
}

func (r *resyncControl) beginPrivateIO(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.activeLocal(ctx); err != nil {
		return err
	}
	// Share the publication admission gate with catalog writer sessions. Abort
	// can still tombstone the attempt without waiting for a blocked upload.
	r.sessions++
	return nil
}

func (r *resyncControl) beginFencedPrivateIO(ctx context.Context) error {
	r.mu.Lock()
	err := r.activeLocal(ctx)
	r.mu.Unlock()
	if err != nil {
		return err
	}
	if err := utils.CheckExecutionFence(ctx); err != nil {
		return err
	}
	// The remote read may have raced abort or publication; admit only after a
	// fresh local check, and never hold mu over the read or underlying FileIO.
	return r.beginPrivateIO(ctx)
}

func (s *nativeResyncIO) Create(path string) (iceio.FileWriter, error) {
	r := s.attempt
	if err := r.beginFencedPrivateIO(s.ctx); err != nil {
		return nil, err
	}
	writable, ok := s.IO.(iceio.WriteFileIO)
	if !ok {
		r.mu.Lock()
		r.sessions--
		r.mu.Unlock()
		return nil, errors.New("native resync table FileIO does not support writing")
	}
	writer, err := writable.Create(path)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sessions--
	if err != nil {
		return nil, err
	}
	if r.owned == nil {
		r.owned = make(map[string]struct{})
	}
	r.owned[path] = struct{}{}
	if err := r.activeLocal(s.ctx); err != nil {
		// Creation may already have produced an object or multipart upload. Do
		// not finalize or remove it after the attempt became unwritable.
		r.failed = true
		return nil, err
	}
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
	// Publication must not pass its zero-session gate while removal is in flight.
	if r.unknown || r.published {
		r.mu.Unlock()
		return errors.New("cannot remove files after a possibly successful resync publication")
	}
	if _, owned := r.owned[path]; !owned {
		r.mu.Unlock()
		return errors.New("cannot remove a file not created by this resync stage")
	}
	r.sessions++
	r.mu.Unlock()
	err := s.IO.Remove(path)
	r.mu.Lock()
	r.sessions--
	r.mu.Unlock()
	return err
}

type nativeResyncFileWriter struct {
	iceio.FileWriter
	mu      sync.Mutex
	attempt *resyncControl
	ctx     context.Context
}

func (w *nativeResyncFileWriter) Write(data []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.attempt.beginPrivateIO(w.ctx); err != nil {
		return 0, err
	}
	n, err := w.FileWriter.Write(data)
	w.attempt.mu.Lock()
	w.attempt.sessions--
	stateErr := w.attempt.activeLocal(w.ctx)
	w.attempt.mu.Unlock()
	if err != nil {
		return n, err
	}
	return n, stateErr
}

func (w *nativeResyncFileWriter) ReadFrom(reader io.Reader) (int64, error) {
	return io.Copy(struct{ io.Writer }{w}, reader)
}

func (w *nativeResyncFileWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	// Close can finalize a multipart upload. Under a revoked fence do not
	// dispatch it; retain the unfinalized upload rather than publish an object.
	if err := w.attempt.beginFencedPrivateIO(w.ctx); err != nil {
		w.attempt.mu.Lock()
		w.attempt.failed = true
		w.attempt.mu.Unlock()
		return err
	}
	err := w.FileWriter.Close()
	w.attempt.mu.Lock()
	w.attempt.sessions--
	stateErr := w.attempt.activeLocal(w.ctx)
	if stateErr != nil {
		w.attempt.failed = true
	}
	w.attempt.mu.Unlock()
	if err != nil {
		return err
	}
	return stateErr
}
