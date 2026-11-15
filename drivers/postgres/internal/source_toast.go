package driver

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/pkg/jdbc"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/jackc/pgx/v5"
)

const sourceToastBatchRows = 128
const sourceToastBatchBytes = 4 * 1024 * 1024

func (c CDC) sourceToastRecovery() bool {
	return c.ToastValueSource == "source"
}

func (c CDC) validateToastRecovery() error {
	switch c.ToastValueSource {
	case "", "destination", "source":
	default:
		return fmt.Errorf("toast_value_source must be destination or source")
	}
	return nil
}

// streamChangesWithToast is shared by normal/recovery and resync CDC. Only successful
// source reads and callbacks may reach PostCDC's slot acknowledgement. Do not flush
// a buffered tail after an error: its containing transaction may be incomplete.
func (p *Postgres) streamChangesWithToast(ctx context.Context, callback abstract.CDCMsgFn) error {
	if !p.cdcConfig.sourceToastRecovery() {
		return p.replicator.StreamChanges(ctx, p.client, callback)
	}
	logger.Warnf("Source TOAST recovery enabled: current CDC-host values may be newer than WAL events; missing rows fail the sync")
	batch := &sourceToastBatch{postgres: p, callback: callback}
	if err := p.replicator.StreamChanges(ctx, p.client, batch.push); err != nil {
		return err
	}
	if p.replicator.Socket().TransactionOpen {
		return fmt.Errorf("source TOAST recovery refused to flush an incomplete WAL transaction")
	}
	return batch.flush(ctx)
}

type sourceToastBatch struct {
	postgres *Postgres
	callback abstract.CDCMsgFn
	pending  []abstract.CDCChange
	bytes    int64
}

func (b *sourceToastBatch) push(ctx context.Context, change abstract.CDCChange) error {
	// Never query a new schema for earlier rows buffered before its relation event.
	if change.SourceColumns != nil {
		if err := b.flush(ctx); err != nil {
			return err
		}
		return b.callback(ctx, change)
	}
	// Do not delay ordinary traffic or allocate a buffer unless hydration is needed.
	if len(b.pending) == 0 && len(change.UnavailableColumns) == 0 {
		return b.callback(ctx, change)
	}
	b.pending = append(b.pending, change)
	b.bytes += change.Bytes
	if len(b.pending) >= sourceToastBatchRows || b.bytes >= sourceToastBatchBytes {
		return b.flush(ctx)
	}
	return nil
}

func (b *sourceToastBatch) flush(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// Groups share a source table and missing-column projection. Keep emission in
	// original WAL order, even when multiple tables/projections share this batch.
	groups := make(map[string][]int)
	var order []string
	for i, change := range b.pending {
		if len(change.UnavailableColumns) == 0 {
			continue
		}
		// JSON tuple encoding avoids delimiter collisions in quoted SQL identifiers.
		encoded, err := json.Marshal(append([]string{change.Stream.Namespace(), change.Stream.Name()}, change.UnavailableColumns...))
		if err != nil {
			return err
		}
		key := string(encoded)
		if _, exists := groups[key]; !exists {
			order = append(order, key)
		}
		groups[key] = append(groups[key], i)
	}
	for _, key := range order {
		if err := b.hydrate(ctx, groups[key]); err != nil {
			return err
		}
	}
	for _, change := range b.pending {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := b.callback(ctx, change); err != nil {
			return err
		}
	}
	b.pending, b.bytes = nil, 0
	return nil
}

func (b *sourceToastBatch) hydrate(ctx context.Context, indices []int) error {
	first := b.pending[indices[0]]
	stream := first.Stream
	if stream.GetStream().SourceDefinedPrimaryKey == nil {
		return fmt.Errorf("source TOAST recovery requires primary keys for stream %s", stream.ID())
	}
	keys := stream.GetStream().SourceDefinedPrimaryKey.Array()
	sort.Strings(keys)
	if len(keys) == 0 {
		return fmt.Errorf("source TOAST recovery requires primary keys for stream %s", stream.ID())
	}
	columns := append(append([]string(nil), keys...), first.UnavailableColumns...)
	selected := make([]string, len(columns))
	for i, column := range columns {
		selected[i] = pgx.Identifier{column}.Sanitize()
	}
	var predicates []string
	var args []any
	requested := make(map[string]bool)
	for _, at := range indices {
		change := b.pending[at]
		for _, key := range keys {
			value, exists := change.Data[key]
			if !exists || value == nil {
				return fmt.Errorf("source TOAST recovery missing primary key column %s for stream %s", key, stream.ID())
			}
		}
		id := utils.GetKeysHash(change.Data, keys...)
		if requested[id] {
			continue
		}
		requested[id] = true
		var terms []string
		for _, key := range keys {
			args = append(args, change.Data[key])
			terms = append(terms, fmt.Sprintf("%s = $%d", pgx.Identifier{key}.Sanitize(), len(args)))
		}
		predicates = append(predicates, "("+strings.Join(terms, " AND ")+")")
	}
	query := "SELECT " + strings.Join(selected, ", ") + " FROM " + pgx.Identifier{stream.Namespace(), stream.Name()}.Sanitize() + " WHERE " + strings.Join(predicates, " OR ")
	// Use the CDC host, never the optional asynchronous backfill reader. Bound
	// lookup latency and honor cancellation. Ordinary SELECT is not an AS OF LSN read.
	lookupCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	rows, err := b.postgres.client.QueryContext(lookupCtx, query, args...)
	if err != nil {
		return fmt.Errorf("source TOAST lookup failed for stream %s: %w", stream.ID(), err)
	}
	defer rows.Close()
	values := make(map[string]map[string]any)
	for rows.Next() {
		row := make(map[string]any)
		if _, err := jdbc.MapScan(rows, row, b.postgres.dataTypeConverter, pgColumnSizer); err != nil {
			return fmt.Errorf("decode source TOAST lookup for stream %s: %w", stream.ID(), err)
		}
		id := utils.GetKeysHash(row, keys...)
		if _, exists := values[id]; exists {
			return fmt.Errorf("source TOAST lookup returned duplicate keys for stream %s", stream.ID())
		}
		values[id] = row
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read source TOAST lookup for stream %s: %w", stream.ID(), err)
	}
	for _, at := range indices {
		change := &b.pending[at]
		row, exists := values[utils.GetKeysHash(change.Data, keys...)]
		if !exists {
			return fmt.Errorf("source TOAST lookup found no row for stream %s; row may have been deleted or its primary key changed; checkpoint was not advanced", stream.ID())
		}
		// Copy the event map: callbacks/decoders may retain it. Explicit source WAL
		// values (including NULL) are never overwritten with a newer database value.
		data := make(map[string]any, len(change.Data))
		for key, value := range change.Data {
			data[key] = value
		}
		for _, column := range change.UnavailableColumns {
			value, exists := row[column]
			if !exists {
				return fmt.Errorf("source TOAST lookup omitted column %s for stream %s", column, stream.ID())
			}
			data[column] = value
		}
		change.Data, change.UnavailableColumns = data, nil
	}
	return nil
}
