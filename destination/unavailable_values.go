package destination

import (
	"fmt"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/errs"
)

// codeUnavailableValueUnrecoverable names a value the source could not send (Postgres unchanged
// TOAST) that the destination could not recover from the row's previous version.
const codeUnavailableValueUnrecoverable = "destination.unavailable_value_unrecoverable"

// RejectUnavailableValues fails the batch when a selected column of a record still holds the
// placeholder for a value the source could not send. Writing the placeholder would replace the
// row's real value, so the sync stops and the row has to be repaired before it continues.
func RejectUnavailableValues(stream types.StreamInterface, records []types.RawRecord) error {
	for _, record := range records {
		for _, column := range record.UnavailableColumns {
			value, selected := record.Data[column]
			if !selected || value != constants.UnavailableValue {
				continue
			}
			// Retrying replays the same change from the same LSN, so it can only fail again.
			return errs.Precondition(errs.DestinationWriteError, codeUnavailableValueUnrecoverable,
				fmt.Errorf("%w: stream %s: unchanged TOAST column %q of row %v (cdc timestamp %v) could not be recovered from the destination; refusing to overwrite it with %s",
					constants.ErrNonRetryable, stream.ID(), column, record.OlakeColumns[constants.OlakeID], record.OlakeColumns[constants.CdcTimestamp], constants.UnavailableValue))
		}
	}
	return nil
}
