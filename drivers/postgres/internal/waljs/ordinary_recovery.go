package waljs

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/jackc/pglogrepl"
)

// SourceIdentity describes the live source, not an invented historical identity.
// Fixed struct field order makes the JSON deterministic and unambiguous even for
// database names containing delimiters. Config hashes bind the retired source.
func SourceIdentity(system pglogrepl.IdentifySystemResult, slot, plugin string) (string, error) {
	if system.SystemID == "" || system.Timeline <= 0 || system.DBName == "" || slot == "" || plugin == "" {
		return "", fmt.Errorf("ordinary recovery requires complete live source identity")
	}
	encoded, err := json.Marshal(struct {
		SystemID string `json:"system_id"`
		Timeline int32  `json:"timeline"`
		Database string `json:"database"`
		SlotName string `json:"slot_name"`
		Plugin   string `json:"plugin"`
	}{system.SystemID, system.Timeline, system.DBName, slot, plugin})
	return string(encoded), err
}

func (p *pgoutputReplicator) changePosition() pglogrepl.LSN {
	if p.socket.StrictRecovery && p.messageLSN != 0 {
		return p.messageLSN
	}
	return p.socket.ClientXLogPos
}

func (p *pgoutputReplicator) bufferRecoveryChange(ctx context.Context, change abstract.CDCChange) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !p.socket.TransactionOpen {
		return fmt.Errorf("ordinary recovery received a change outside a complete transaction")
	}
	p.recoveryPending = append(p.recoveryPending, change)
	return nil
}

func (p *pgoutputReplicator) processRecoveryBoundary(ctx context.Context, logical any, callback abstract.CDCMsgFn) (bool, error) {
	switch msg := logical.(type) {
	case *pglogrepl.BeginMessage:
		if p.socket.TransactionOpen || len(p.recoveryPending) != 0 {
			return true, fmt.Errorf("ordinary recovery received a nested or incomplete transaction")
		}
		if msg.FinalLSN > p.socket.CurrentWalPosition {
			// All earlier commits have been decoded; this transaction commits
			// strictly after the bound. Do not emit even its first row/schema.
			p.socket.ClientXLogPos = p.socket.CurrentWalPosition
			return true, nil
		}
		if msg.FinalLSN == p.socket.CurrentWalPosition {
			return true, fmt.Errorf("ordinary recovery target falls inside a transaction commit record")
		}
		p.socket.TransactionOpen = true
		p.txnCommitTime = msg.CommitTime
		p.txnFinalLSN = msg.FinalLSN
		if p.progress != nil {
			p.progress.transactionOpen.Store(true)
		}
		return true, nil
	case *pglogrepl.CommitMessage:
		if !p.socket.TransactionOpen || msg.CommitLSN != p.txnFinalLSN || msg.TransactionEndLSN > p.socket.CurrentWalPosition || msg.TransactionEndLSN <= p.socket.ClientXLogPos {
			return true, fmt.Errorf("ordinary recovery requires a complete transaction at or before its exact target")
		}
		// Unlike normal CDC, buffering is necessary here: BEGIN has the commit
		// record's start LSN, not its end. No row may escape a crossing commit.
		for _, change := range p.recoveryPending {
			if err := ctx.Err(); err != nil {
				return true, err
			}
			if err := callback(ctx, change); err != nil {
				return true, err
			}
		}
		clear(p.recoveryPending)
		p.recoveryPending = p.recoveryPending[:0]
		p.socket.TransactionOpen = false
		p.socket.ClientXLogPos = msg.TransactionEndLSN
		if p.progress != nil {
			p.progress.committed(msg.TransactionEndLSN)
		}
		return true, nil
	default:
		return false, nil
	}
}

func (s *Socket) observeKeepalive(sent pglogrepl.LSN) {
	if !s.StrictRecovery {
		s.ClientXLogPos = sent
		return
	}
	// PostgreSQL logical walsender keepalives report processed/sent WAL,
	// unlike XLogData.ServerWALEnd or IdentifySystem's live tip. See
	// src/backend/replication/walsender.c: XLogSendLogical/WalSndKeepalive.
	// Never cross an open transaction or let later WAL widen the bound.
	if !s.TransactionOpen {
		s.ClientXLogPos = max(s.ClientXLogPos, min(sent, s.CurrentWalPosition))
	}
}
