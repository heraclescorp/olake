package waljs

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/datazip-inc/olake/utils/typeutils"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jmoiron/sqlx"
)

// pgoutputReplicator implements Replicator for pgoutput
type pgoutputReplicator struct {
	socket              *Socket
	publication         string
	txnCommitTime       time.Time // transaction commit time
	txnFinalLSN         pglogrepl.LSN
	relationIDToMsgMap  map[uint32]*pglogrepl.RelationMessage // map to store relation id
	typeNames           map[uint32]relationTypeNames          // converter type names of each relation's columns
	progress            *replicationProgress
	typeMap             *pgtype.Map
	preserveToastValues bool     // mark columns Postgres left out of an UPDATE so the destination can recover them
	db                  *sqlx.DB // regular connection for catalog reads; the replication connection cannot run queries mid-stream
	// Recovery holds one transaction until COMMIT proves it is wholly in range.
	recoveryPending []abstract.CDCChange
	messageLSN      pglogrepl.LSN
}

func (p *pgoutputReplicator) Socket() *Socket {
	return p.socket
}

func (p *pgoutputReplicator) StreamChanges(ctx context.Context, db *sqlx.DB, insertFn abstract.CDCMsgFn) error {
	p.db = db
	if !p.socket.StrictRecovery {
		if err := p.emitSyncStartColumns(ctx, insertFn); err != nil {
			return err
		}
	}

	if err := utils.CheckExecutionFence(ctx); err != nil {
		return err
	}
	err := pglogrepl.StartReplication(ctx, p.socket.pgConn, fmt.Sprintf("%q", p.socket.ReplicationSlot), p.socket.ConfirmedFlushLSN, pglogrepl.StartReplicationOptions{
		PluginArgs: []string{"proto_version '1'", fmt.Sprintf("publication_names '%s'", p.publication)}})
	if err != nil {
		return fmt.Errorf("failed to start replication: %w", err)
	}

	logger.Infof("pgoutput starting from lsn=%s target=%s", p.socket.ConfirmedFlushLSN, p.socket.CurrentWalPosition)
	progress, stopProgress := startReplicationProgress(p.socket, "pgoutput")
	p.progress = progress
	defer func() { stopProgress(); p.progress = nil }()
	if p.preserveToastValues {
		logger.Info("preserve_toast_values enabled: unavailable columns are marked for recovery by the configured TOAST value source")
	}

	cdcStartTime := time.Now()
	messageReceived := false
	// A run may stop only between complete transactions.
	p.socket.TransactionOpen = false

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			if !messageReceived && p.socket.initialWaitTime > 0 && time.Since(cdcStartTime) > p.socket.initialWaitTime {
				return fmt.Errorf("%w, try increasing it or do full load", constants.ErrNonRetryable)
			}

			if !p.socket.TransactionOpen && p.socket.ClientXLogPos >= p.socket.CurrentWalPosition {
				logger.Infof("finishing sync, reached wal position: %s", p.socket.CurrentWalPosition)
				return nil
			}

			// receive message with timeout
			msgCtx, cancel := context.WithTimeout(ctx, p.socket.initialWaitTime)
			msg, err := p.socket.pgConn.ReceiveMessage(msgCtx)
			cancel()
			if err != nil {
				if errors.Is(err, context.DeadlineExceeded) {
					return fmt.Errorf("%w: no records found in given initial wait time, try increasing it or do full load", constants.ErrNonRetryable)
				}

				if errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "EOF") {
					return nil
				}
				return err
			}

			copyData, ok := msg.(*pgproto3.CopyData)
			if !ok {
				return fmt.Errorf("pgoutput unexpected message type: %T", msg)
			}

			switch copyData.Data[0] {
			case pglogrepl.XLogDataByteID:
				xld, err := pglogrepl.ParseXLogData(copyData.Data[1:])
				if err != nil {
					return fmt.Errorf("failed to parse XLogData: %w", err)
				}
				progress.received(xld.WALStart, xld.ServerWALEnd, len(xld.WALData))
				p.messageLSN = xld.WALStart
				if !p.socket.StrictRecovery {
					p.socket.ClientXLogPos = xld.WALStart
				}
				if err := p.processPgoutputWAL(ctx, xld.WALData, insertFn); err != nil {
					return err
				}
				messageReceived = true
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(copyData.Data[1:])
				if err != nil {
					return fmt.Errorf("failed to parse primary keepalive message: %w", err)
				}
				progress.serverEnd.Store(uint64(pkm.ServerWALEnd))
				p.socket.observeKeepalive(pkm.ServerWALEnd)
				if pkm.ReplyRequested {
					if err := AcknowledgeLSN(ctx, db, p.socket, true); err != nil {
						return fmt.Errorf("failed to send standby status update: %w", err)
					}
				}
			default:
				logger.Debugf("pgoutput: unhandled message type: %d", copyData.Data[0])
			}
		}
	}
}

// TODO: can we parallelize this function
func (p *pgoutputReplicator) processPgoutputWAL(ctx context.Context, walData []byte, insertFn abstract.CDCMsgFn) error {
	logicalMsg, err := pglogrepl.Parse(walData)
	if err != nil {
		return fmt.Errorf("failed to parse WAL data: %w", err)
	}

	if p.socket.StrictRecovery {
		if handled, err := p.processRecoveryBoundary(ctx, logicalMsg, insertFn); handled {
			return err
		}
		insertFn = p.bufferRecoveryChange
	}
	switch msg := logicalMsg.(type) {
	case *pglogrepl.RelationMessage:
		prev, relationVisited := p.relationIDToMsgMap[msg.RelationID]
		if !relationVisited && msg.ReplicaIdentity != 'f' {
			if p.preserveToastValues {
				logger.Infof("table[%s.%s] replica identity is not FULL; PostgreSQL may omit unchanged TOAST values, which are marked for recovery by the configured TOAST value source", msg.Namespace, msg.RelationName)
			} else {
				logger.Warnf("table[%s.%s] replica identity is not FULL and TOAST recovery is disabled; UPDATE events with omitted unchanged TOAST values will fail the sync", msg.Namespace, msg.RelationName)
			}
		}
		p.relationIDToMsgMap[msg.RelationID] = msg
		if !relationColumnsChanged(prev, msg) {
			return nil
		}
		return p.emitRelationColumns(ctx, msg, insertFn)
	case *pglogrepl.BeginMessage:
		p.socket.TransactionOpen = true
		if p.progress != nil {
			p.progress.transactionOpen.Store(true)
		}
		p.txnCommitTime = msg.CommitTime
		p.txnFinalLSN = msg.FinalLSN
		return nil
	case *pglogrepl.InsertMessage:
		return p.emitInsert(ctx, msg, insertFn)
	case *pglogrepl.UpdateMessage:
		return p.emitUpdate(ctx, msg, insertFn)
	case *pglogrepl.DeleteMessage:
		return p.emitDelete(ctx, msg, insertFn)
	case *pglogrepl.CommitMessage:
		p.socket.TransactionOpen = false
		p.socket.ClientXLogPos = msg.TransactionEndLSN
		if p.progress != nil {
			p.progress.committed(msg.TransactionEndLSN)
		}
		return nil
	default:
		return nil
	}
}

// tupleValuesToMap converts one row's tuple into column values. The returned columns are
// this row's out-of-line values the UPDATE left unchanged, which Postgres did not send;
// nil unless preserveToastValues is on.
func (p *pgoutputReplicator) tupleValuesToMap(rel *pglogrepl.RelationMessage, tuple, oldTuple *pglogrepl.TupleData) (map[string]any, int64, []string, error) {
	data := make(map[string]any)
	if tuple == nil {
		return data, 0, nil, nil
	}

	// rowBytes is summed inline below (attached to CDCChange.Bytes by the caller),
	// so the tuple is walked only once.
	var rowBytes int64
	var unchangedToastColumns []string

	typeNames := p.columnTypeNames(rel)
	for idx, col := range tuple.Columns {
		if idx >= len(rel.Columns) {
			continue
		}
		colName := rel.Columns[idx].Name
		colType := rel.Columns[idx].DataType

		// Size the data that actually arrived in this WAL change (the new tuple),
		// before any TOAST recovery below. Unchanged-TOAST columns carry nil Data
		// here and add 0 — Postgres does not resend them.
		if col.Data != nil {
			rowBytes += oidStorageBytes(colType, col.Data)
		}

		isUnchangedToast := col.DataType == pglogrepl.TupleDataTypeToast
		// On UPDATE, unchanged TOAST columns in the new tuple are marked TupleDataTypeToast.
		// REPLICA IDENTITY FULL includes the complete old row and allows recovery of these values.
		// DEFAULT, INDEX, and NOTHING do not provide old TOAST values, so recovery is not possible.
		if isUnchangedToast && oldTuple != nil && idx < len(oldTuple.Columns) {
			col = oldTuple.Columns[idx]
		}
		if col.Data == nil {
			if isUnchangedToast && !p.preserveToastValues {
				// Writing the placeholder would replace the row's real value in the destination.
				return nil, 0, nil, fmt.Errorf("%w: table[%s.%s] column[%s]: postgres did not send an unchanged TOAST value (replica identity is not FULL); TOAST recovery must be enabled by the PostgreSQL driver",
					constants.ErrNonRetryable, rel.Namespace, rel.RelationName, colName)
			}
			// If the column is a TOAST column, set the value to __olake_unavailable_value__ otherwise set it to nil
			data[colName] = utils.Ternary(isUnchangedToast, constants.UnavailableValue, nil)
			if isUnchangedToast {
				unchangedToastColumns = append(unchangedToastColumns, colName)
			}
			continue
		}

		typeName := typeNames[idx]
		var value any
		if colType == pgtype.VarcharArrayOID {
			if p.typeMap == nil {
				p.typeMap = pgtype.NewMap()
			}
			var format int16 = pgtype.TextFormatCode
			if col.DataType == pglogrepl.TupleDataTypeBinary {
				format = pgtype.BinaryFormatCode
			}
			var err error
			arrayType, _ := p.typeMap.TypeForOID(colType)
			value, err = arrayType.Codec.DecodeValue(p.typeMap, colType, format, col.Data)
			if err != nil {
				return nil, 0, nil, fmt.Errorf("failed to decode varchar array column[%s]: %w", colName, err)
			}
		} else if colType == pgtype.ByteaOID && utils.IsBinarySupported() {
			var format int16 = pgtype.TextFormatCode
			if col.DataType == pglogrepl.TupleDataTypeBinary {
				format = pgtype.BinaryFormatCode
			}
			var err error
			value, err = (pgtype.ByteaCodec{}).DecodeValue(nil, colType, format, col.Data)
			if err != nil {
				return nil, 0, nil, fmt.Errorf("failed to decode bytea column[%s]: %w", colName, err)
			}
		} else {
			value = string(col.Data)
		}
		val, err := p.socket.changeFilter.converter(value, typeName)
		if err != nil && err != typeutils.ErrNullValue {
			return nil, 0, nil, err
		}
		data[colName] = val
	}
	return data, rowBytes, unchangedToastColumns, nil
}

func (p *pgoutputReplicator) emitInsert(ctx context.Context, m *pglogrepl.InsertMessage, insertFn abstract.CDCMsgFn) error {
	rel, ok := p.relationIDToMsgMap[m.RelationID]
	if !ok {
		return fmt.Errorf("unknown relation id: %d", m.RelationID)
	}

	stream := p.socket.changeFilter.tables[fmt.Sprintf("%s.%s", rel.Namespace, rel.RelationName)]
	if stream == nil {
		return nil
	}
	if p.socket.changeFilter.skipTransaction(stream.ID(), p.txnFinalLSN) {
		return nil
	}

	values, rowBytes, _, err := p.tupleValuesToMap(rel, m.Tuple, nil)
	if err != nil {
		return err
	}

	return insertFn(ctx, abstract.NewCDCChange(stream, p.txnCommitTime, "insert", values,
		map[string]any{CDCLSN: p.changePosition().String()}, rowBytes, nil))
}

func (p *pgoutputReplicator) emitUpdate(ctx context.Context, m *pglogrepl.UpdateMessage, insertFn abstract.CDCMsgFn) error {
	rel, ok := p.relationIDToMsgMap[m.RelationID]
	if !ok {
		return fmt.Errorf("unknown relation id: %d", m.RelationID)
	}

	stream := p.socket.changeFilter.tables[fmt.Sprintf("%s.%s", rel.Namespace, rel.RelationName)]
	if stream == nil {
		return nil
	}
	if p.socket.changeFilter.skipTransaction(stream.ID(), p.txnFinalLSN) {
		return nil
	}

	values, rowBytes, unchangedToastColumns, err := p.tupleValuesToMap(rel, m.NewTuple, m.OldTuple)
	if err != nil {
		return err
	}

	return insertFn(ctx, abstract.NewCDCChange(stream, p.txnCommitTime, "update", values,
		map[string]any{CDCLSN: p.changePosition().String()}, rowBytes, unchangedToastColumns))
}

func (p *pgoutputReplicator) emitDelete(ctx context.Context, m *pglogrepl.DeleteMessage, insertFn abstract.CDCMsgFn) error {
	rel, ok := p.relationIDToMsgMap[m.RelationID]
	if !ok {
		return fmt.Errorf("unknown relation id: %d", m.RelationID)
	}

	stream := p.socket.changeFilter.tables[fmt.Sprintf("%s.%s", rel.Namespace, rel.RelationName)]
	if stream == nil {
		return nil
	}
	if p.socket.changeFilter.skipTransaction(stream.ID(), p.txnFinalLSN) {
		return nil
	}

	values, rowBytes, _, err := p.tupleValuesToMap(rel, m.OldTuple, nil)
	if err != nil {
		return err
	}

	return insertFn(ctx, abstract.NewCDCChange(stream, p.txnCommitTime, "delete", values,
		map[string]any{CDCLSN: p.changePosition().String()}, rowBytes, nil))
}

// columnTypeNames returns the converter's type name for each column of rel, built once per
// Relation message so no value pays for it.
func (p *pgoutputReplicator) columnTypeNames(rel *pglogrepl.RelationMessage) []string {
	if cached, ok := p.typeNames[rel.RelationID]; ok && cached.relation == rel {
		return cached.names
	}
	names := make([]string, len(rel.Columns))
	for idx, column := range rel.Columns {
		names[idx] = columnTypeName(column.DataType, column.TypeModifier)
	}
	if p.typeNames == nil {
		p.typeNames = make(map[uint32]relationTypeNames)
	}
	p.typeNames[rel.RelationID] = relationTypeNames{relation: rel, names: names}
	return names
}

// relationTypeNames holds the converter's type names for the columns of one Relation message.
type relationTypeNames struct {
	relation *pglogrepl.RelationMessage
	names    []string
}

// columnTypeName is the converter's type name for a column of type oid and type modifier typmod.
// A numeric with a declared precision and scale is named by the decimal(p,s) it holds; an
// unconstrained numeric (typmod -1), or one Iceberg cannot hold as a decimal, stays numeric.
func columnTypeName(oid uint32, typmod int32) string {
	if oid == pgtype.NumericOID {
		if precision, scale, constrained := numericTypmod(typmod); constrained {
			if decimal, ok := types.DecimalOf(precision, scale); ok {
				return string(decimal)
			}
		}
	}
	return oidToString(oid)
}

// numericTypmod decodes a numeric type modifier, (precision<<16 | scale) + VARHDRSZ (4). An
// unconstrained numeric has typmod -1.
func numericTypmod(typmod int32) (precision, scale int, constrained bool) {
	if typmod < 4 {
		return 0, 0, false
	}
	mod := typmod - 4
	return int((mod >> 16) & 0xffff), int(mod & 0xffff), true
}

// OIDToString converts a PostgreSQL OID to its string representation
func oidToString(oid uint32) string {
	if typeName, ok := oidToTypeName[oid]; ok {
		return typeName
	}
	logger.Warnf("unknown oid[%d] falling back to string", oid)
	// default to json, which will be converted to string
	return "json"
}

// OidToTypeName maps PostgreSQL OIDs to their corresponding type names
var oidToTypeName = map[uint32]string{
	pgtype.BoolOID:             "bool",
	pgtype.ByteaOID:            "bytea",
	pgtype.Int8OID:             "int8",
	pgtype.Int2OID:             "int2",
	pgtype.Int4OID:             "int4",
	pgtype.TextOID:             "text",
	pgtype.UUIDOID:             "uuid",
	pgtype.JSONOID:             "json",
	pgtype.Float4OID:           "float4",
	pgtype.Float8OID:           "float8",
	pgtype.BoolArrayOID:        "bool[]",
	pgtype.Int2ArrayOID:        "int2[]",
	pgtype.Int4ArrayOID:        "int4[]",
	pgtype.TextArrayOID:        "text[]",
	pgtype.VarcharArrayOID:     "varchar[]",
	pgtype.ByteaArrayOID:       "bytea[]",
	pgtype.Int8ArrayOID:        "int8[]",
	pgtype.Float4ArrayOID:      "float4[]",
	pgtype.Float8ArrayOID:      "float8[]",
	pgtype.BPCharOID:           "bpchar",
	pgtype.VarcharOID:          "varchar",
	pgtype.DateOID:             "date",
	pgtype.TimeOID:             "time",
	pgtype.TimestampOID:        "timestamp",
	pgtype.TimestampArrayOID:   "timestamp[]",
	pgtype.DateArrayOID:        "date[]",
	pgtype.TimestamptzOID:      "timestamptz",
	pgtype.TimestamptzArrayOID: "timestamptz[]",
	pgtype.IntervalOID:         "interval",
	pgtype.NumericArrayOID:     "numeric[]",
	pgtype.BitOID:              "bit",
	pgtype.VarbitOID:           "varbit",
	pgtype.NumericOID:          "numeric",
	pgtype.UUIDArrayOID:        "uuid[]",
	pgtype.JSONBOID:            "jsonb",
	pgtype.JSONBArrayOID:       "jsonb[]",
}
