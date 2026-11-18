package types

import "context"

// OrdinaryRecoveryOrigin binds recovery to immutable artifacts from one retired
// controlled sync. Configuration digests hash exact bytes; state uses canonical JSON.
type OrdinaryRecoveryOrigin struct {
	Version               int    `json:"version"`
	OriginalExecutionID   string `json:"original_execution_id"`
	OriginalLaunchID      string `json:"original_launch_id"`
	OriginalReceiptSHA256 string `json:"original_receipt_sha256"`
	SourceSHA256          string `json:"source_sha256"`
	DestinationSHA256     string `json:"destination_sha256"`
	CatalogSHA256         string `json:"catalog_sha256"`
	BaselineStateSHA256   string `json:"baseline_state_sha256"`
}

type OrdinaryRecoveryTableProof struct {
	Stream          string `json:"stream"`
	TableUUID       string `json:"table_uuid"`
	FenceSnapshotID int64  `json:"fence_snapshot_id"`
	CommittedLSN    string `json:"committed_lsn"`
}

type OrdinaryRecoveryResult struct {
	OrdinaryRecoveryOrigin
	ExecutionID         string                       `json:"execution_id"`
	LaunchID            string                       `json:"launch_id"`
	StateSHA256         string                       `json:"state_sha256"`
	LowerLSN            string                       `json:"lower_lsn"`
	TargetLSN           string                       `json:"target_lsn"`
	ServerIdentity      string                       `json:"server_identity"`
	SlotName            string                       `json:"slot_name"`
	ToastValueSemantics string                       `json:"toast_value_semantics"`
	Tables              []OrdinaryRecoveryTableProof `json:"tables"`
}

// OrdinaryRecoveryPlan is derived solely from authoritative selected table metadata.
type OrdinaryRecoveryPlan struct {
	LowerLSN    string
	TargetLSN   string
	ReplayAfter map[string]string
}

type OrdinaryRecoveryOptions struct {
	Origin         OrdinaryRecoveryOrigin
	SourceIdentity string
	Plan           *OrdinaryRecoveryPlan
}

type ordinaryRecoveryKey struct{}

func WithOrdinaryRecovery(ctx context.Context, options *OrdinaryRecoveryOptions) context.Context {
	return context.WithValue(ctx, ordinaryRecoveryKey{}, options)
}

func OrdinaryRecoveryFromContext(ctx context.Context) (*OrdinaryRecoveryOptions, bool) {
	options, ok := ctx.Value(ordinaryRecoveryKey{}).(*OrdinaryRecoveryOptions)
	return options, ok && options != nil
}
