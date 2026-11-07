package abstract

import (
	"context"

	"github.com/datazip-inc/olake/types"
)

// TableEnrollmentSource is an optional, metadata-only capability. Drivers that sample
// records cannot accidentally opt in by implementing the normal discovery interface.
// SetupTableEnrollment must not create/advance slots, publications, or source data.
type TableEnrollmentSource interface {
	SetupTableEnrollment(context.Context) error
	GetStreamNames(context.Context) ([]types.StreamID, error)
	ProduceSchema(context.Context, types.StreamID) (*types.Stream, error)
	ValidateTableEnrollment(context.Context, *types.Stream) error
	CloseConnection()
}

func (a *AbstractDriver) TableEnrollmentSource() (TableEnrollmentSource, bool) {
	source, ok := a.driver.(TableEnrollmentSource)
	return source, ok
}
