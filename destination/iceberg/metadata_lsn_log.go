package iceberg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/apache/iceberg-go/catalog"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/logger"
)

// Extract only the LSN, never the opaque recovery payload (which can contain
// other drivers' cursor values). Diagnostics must not change recovery behavior.
func nativeMetadataLSNPosition(recovery string) (string, string) {
	if recovery == "" {
		return "absent", ""
	}
	var metadata types.MetadataState
	if err := json.Unmarshal([]byte(recovery), &metadata); err != nil {
		return "invalid_recovery", ""
	}
	if metadata.State == nil {
		return "no_cdc_state", ""
	}
	state, ok := metadata.State.(string)
	if !ok {
		return "invalid_state_type", ""
	}
	var position struct {
		LSN string `json:"lsn"`
	}
	if err := json.Unmarshal([]byte(state), &position); err != nil {
		return "invalid_state", ""
	}
	if position.LSN == "" {
		return "no_lsn", ""
	}
	parts := strings.Split(position.LSN, "/")
	if len(parts) != 2 {
		return "invalid_lsn", ""
	}
	high, highErr := strconv.ParseUint(parts[0], 16, 32)
	low, lowErr := strconv.ParseUint(parts[1], 16, 32)
	if highErr != nil || lowErr != nil {
		return "invalid_lsn", ""
	}
	return "present", fmt.Sprintf("%X/%X", high, low)
}

func logNativeMetadataLSN(stream, tableUUID string, snapshot int64, recovery string) {
	status, lsn := nativeMetadataLSNPosition(recovery)
	logger.Infof("iceberg_metadata_cdc_position stream=%q table_uuid=%q snapshot_id=%d metadata_status=%q metadata_lsn=%q phase=table_load", stream, tableUUID, snapshot, status, lsn)
}

// LogMetadataPosition reads only the published table, not a resync staging table.
// Failures are diagnostic, not evidence of absent recovery state. Never log the
// raw error or properties: catalog errors can include credentials/response bodies.
func (i *Iceberg) LogMetadataPosition(ctx context.Context, stream types.StreamInterface) {
	identifier := catalog.ToIdentifier(stream.GetDestinationDatabase(&i.config.IcebergDatabase), stream.GetDestinationTable())
	tbl, err := i.catalog.LoadTable(nativeCatalogContext(ctx, i.catalog), identifier)
	if err != nil {
		status := "lookup_failed"
		if errors.Is(err, catalog.ErrNoSuchTable) {
			status = "table_absent"
		}
		if ctx.Err() != nil {
			status = "lookup_canceled_or_timed_out"
		}
		logger.Warnf("iceberg_metadata_cdc_position stream=%q metadata_status=%q phase=pre_cdc", stream.ID(), status)
		return
	}
	status, lsn := nativeMetadataLSNPosition(tbl.Properties()[nativeRecoveryProperty])
	logger.Infof("iceberg_metadata_cdc_position stream=%q table_uuid=%q snapshot_id=%d metadata_status=%q metadata_lsn=%q phase=pre_cdc", stream.ID(), tbl.Metadata().TableUUID().String(), nativeSnapshotID(tbl), status, lsn)
}
