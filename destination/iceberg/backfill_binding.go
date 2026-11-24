package iceberg

import (
	"errors"
	"slices"

	apacheiceberg "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/datazip-inc/olake/types"
)

type backfillHistoricalMetadata struct {
	table.Metadata
	schemas    []*apacheiceberg.Schema
	current    *apacheiceberg.Schema
	lastColumn int
}

func (m backfillHistoricalMetadata) Schemas() []*apacheiceberg.Schema     { return m.schemas }
func (m backfillHistoricalMetadata) CurrentSchema() *apacheiceberg.Schema { return m.current }
func (m backfillHistoricalMetadata) LastColumnID() int                    { return m.lastColumn }

func validateBackfillTableBinding(metadata table.Metadata, expected types.BackfillTableBinding) error {
	if expected.TableUUID != metadata.TableUUID().String() {
		return errors.New("immutable backfill binding table UUID or shape changed")
	}
	shape, err := ordinaryRecoveryShape(metadata)
	if err != nil {
		return err
	}
	if shape == expected.ShapeSHA256 {
		return nil
	}
	schemas := metadata.Schemas()
	if len(schemas) < 2 || schemas[len(schemas)-1] == nil || metadata.CurrentSchema() == nil || metadata.CurrentSchema().ID != schemas[len(schemas)-1].ID {
		return errors.New("immutable backfill binding has no complete historical schema proof")
	}
	highest := 0
	for end, schema := range schemas {
		if schema == nil || (end > 0 && schema.ID <= schemas[end-1].ID) {
			return errors.New("immutable backfill binding has invalid retained schema history")
		}
		highest = max(highest, schema.HighestFieldID())
		if end == len(schemas)-1 {
			break
		}
		for _, current := range schemas[:end+1] {
			historical := backfillHistoricalMetadata{Metadata: metadata, schemas: schemas[:end+1], current: current, lastColumn: highest}
			shape, err := ordinaryRecoveryShape(historical)
			if err != nil {
				return err
			}
			if shape != expected.ShapeSHA256 {
				continue
			}
			previous, lastColumn, lastSchema := current, highest, schema.ID
			for _, next := range schemas[end+1:] {
				if next == nil || next.ID != lastSchema+1 {
					return errors.New("immutable backfill binding is missing intermediate schema proof")
				}
				if err := validateBackfillSchemaAddition(previous, next, lastColumn); err != nil {
					return err
				}
				previous, lastColumn, lastSchema = next, max(lastColumn, next.HighestFieldID()), next.ID
			}
			if metadata.LastColumnID() != lastColumn {
				return errors.New("immutable backfill binding highest column ID differs from retained schema proof")
			}
			return nil
		}
	}
	return errors.New("immutable backfill binding table UUID or shape changed without historical schema proof")
}

func validateBackfillSchemaAddition(previous, next *apacheiceberg.Schema, lastColumn int) error {
	if !slices.Equal(previous.IdentifierFieldIDs, next.IdentifierFieldIDs) {
		return errors.New("immutable backfill binding schema identifier fields changed")
	}
	before, after := previous.Fields(), next.Fields()
	if len(after) < len(before) {
		return errors.New("immutable backfill binding schema removed fields")
	}
	for index := range before {
		if !before[index].Equals(after[index]) {
			return errors.New("immutable backfill binding schema changed existing fields")
		}
	}
	for _, added := range after[len(before):] {
		if added.Required || added.InitialDefault != nil || added.WriteDefault != nil || added.ID <= lastColumn {
			return errors.New("immutable backfill binding permits only fresh nullable top-level additions without defaults")
		}
		if _, found := previous.FindFieldByName(added.Name); found {
			return errors.New("immutable backfill binding schema reused an existing field name")
		}
	}
	fields, err := next.FlatFields()
	if err != nil {
		return err
	}
	for field := range fields {
		if _, existing := previous.FindFieldByID(field.ID); !existing && (field.ID <= lastColumn || field.InitialDefault != nil || field.WriteDefault != nil) {
			return errors.New("immutable backfill binding schema reused a historical field ID or added defaults")
		}
	}
	return nil
}
