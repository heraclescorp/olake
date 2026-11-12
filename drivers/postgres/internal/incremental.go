package driver

import (
	"context"
	"fmt"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/pkg/jdbc"
	"github.com/datazip-inc/olake/types"
)

func (p *Postgres) StreamIncrementalChanges(ctx context.Context, stream types.StreamInterface, processFn abstract.BackfillMsgFn) error {
	opts := jdbc.DriverOptions{
		Driver: constants.Postgres,
		Stream: stream,
		State:  p.state,
	}
	incrementalQuery, queryArgs, err := jdbc.BuildIncrementalQuery(ctx, opts)
	if err != nil {
		return fmt.Errorf("failed to build incremental condition: %w", err)
	}

	query := p.client.QueryContext
	if p.resync != nil && p.resync.streamID == stream.ID() {
		conn, err := p.backfillConn(ctx, stream)
		if err != nil {
			return err
		}
		defer closeBackfillConn(conn)
		tx, err := p.beginBackfillTx(ctx, conn, stream)
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback() }()
		query = tx.QueryContext
	}
	rows, err := query(ctx, incrementalQuery, queryArgs...)
	if err != nil {
		return fmt.Errorf("failed to execute incremental query: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		record := make(types.Record)
		rowBytes, err := jdbc.MapScan(rows, record, p.dataTypeConverter, pgColumnSizer)
		if err != nil {
			return fmt.Errorf("failed to scan record: %w", err)
		}

		if err := processFn(ctx, record, rowBytes); err != nil {
			return fmt.Errorf("process error: %w", err)
		}
	}

	return rows.Err()
}

func (p *Postgres) FetchMaxCursorValues(ctx context.Context, stream types.StreamInterface) (any, any, error) {
	if p.resync != nil && p.resync.streamID == stream.ID() {
		conn, err := p.backfillConn(ctx, stream)
		if err != nil {
			return nil, nil, err
		}
		defer closeBackfillConn(conn)
		tx, err := p.beginBackfillTx(ctx, conn, stream)
		if err != nil {
			return nil, nil, err
		}
		defer func() { _ = tx.Rollback() }()
		primary, secondary := stream.Cursor()
		query := "SELECT MAX(" + jdbc.QuoteIdentifier(primary, constants.Postgres) + ")"
		if secondary != "" {
			query += ", MAX(" + jdbc.QuoteIdentifier(secondary, constants.Postgres) + ")"
		}
		query += " FROM " + jdbc.QuoteTable(stream.Namespace(), stream.Name(), constants.Postgres)
		var first, second any
		values := []any{&first}
		if secondary != "" {
			values = append(values, &second)
		}
		if err := tx.QueryRowContext(ctx, query).Scan(values...); err != nil {
			return nil, nil, err
		}
		if value, ok := first.([]byte); ok {
			first = string(value)
		}
		if value, ok := second.([]byte); ok {
			second = string(value)
		}
		return first, second, nil
	}
	maxPrimaryCursorValue, maxSecondaryCursorValue, err := jdbc.GetMaxCursorValues(ctx, p.client, constants.Postgres, stream)
	if err != nil {
		return nil, nil, err
	}
	return maxPrimaryCursorValue, maxSecondaryCursorValue, nil
}
