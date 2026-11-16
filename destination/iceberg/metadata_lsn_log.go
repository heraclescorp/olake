package iceberg

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

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
