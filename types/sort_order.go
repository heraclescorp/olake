package types

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/goccy/go-json"
)

// SortOrder is nil when a stream inherits the catalog policy, and non-nil empty
// when it explicitly opts out. Columns name source fields, not Iceberg field IDs.
type SortOrder []SortField

type SortField struct {
	Column    string `json:"column"`
	Direction string `json:"direction"`
	NullOrder string `json:"null_order"`
}

func (o *SortOrder) UnmarshalJSON(data []byte) error {
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return fmt.Errorf("sort order must be an array; omit it to inherit")
	}
	var fields []SortField
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	order := SortOrder(fields)
	if err := order.Validate(); err != nil {
		return err
	}
	*o = order
	return nil
}

func (o SortOrder) Validate() error {
	for idx, field := range o {
		if strings.TrimSpace(field.Column) == "" {
			return fmt.Errorf("sort field %d requires a column", idx+1)
		}
		if field.Direction != "asc" && field.Direction != "desc" {
			return fmt.Errorf("sort column %q direction must be asc or desc", field.Column)
		}
		if field.NullOrder != "nulls-first" && field.NullOrder != "nulls-last" {
			return fmt.Errorf("sort column %q null_order must be nulls-first or nulls-last", field.Column)
		}
		for _, previous := range o[:idx] {
			if previous.Column == field.Column {
				return fmt.Errorf("duplicate sort column %q", field.Column)
			}
		}
	}
	return nil
}

func (o SortOrder) ValidateDefault() error {
	if err := o.Validate(); err != nil {
		return err
	}
	if len(o) > 0 && (len(o) != 1 || o[0] != (SortField{Column: "created_at", Direction: "asc", NullOrder: "nulls-last"})) {
		return fmt.Errorf("default_sort_order must be empty or created_at asc nulls-last")
	}
	return nil
}

// MarshalJSON preserves [] as an explicit opt-out while omitting inherited orders.
func (s StreamMetadata) MarshalJSON() ([]byte, error) {
	type metadata StreamMetadata
	var order *SortOrder
	if s.SortOrder != nil {
		order = &s.SortOrder
	}
	return json.Marshal(struct {
		metadata
		SortOrder *SortOrder `json:"sort_order,omitempty"`
	}{metadata(s), order})
}

func (s LegacyStreamMetadata) MarshalJSON() ([]byte, error) {
	type metadata LegacyStreamMetadata
	var order *SortOrder
	if s.SortOrder != nil {
		order = &s.SortOrder
	}
	return json.Marshal(struct {
		metadata
		SortOrder *SortOrder `json:"sort_order,omitempty"`
	}{metadata(s), order})
}

// ResolveSortOrder validates explicit orders strictly. The opt-in catalog preset
// only applies to eligible normalized Iceberg streams; ineligible streams stay unsorted.
// The returned columns are destination names, with inheritance left intact on disk.
func (s *ConfiguredStream) ResolveSortOrder(destinationType DestinationType) (SortOrder, error) {
	if err := s.DefaultSortOrder.ValidateDefault(); err != nil {
		return nil, err
	}
	order := s.StreamMetadata.SortOrder
	inherited := order == nil
	if inherited {
		order = s.DefaultSortOrder
	}
	if err := order.Validate(); err != nil {
		return nil, err
	}
	if len(order) == 0 {
		return nil, nil
	}
	if destinationType != Iceberg || !s.NormalizationEnabled() {
		if inherited {
			return nil, nil
		}
		return nil, fmt.Errorf("sort_order requires a normalized Iceberg stream")
	}

	resolved := make(SortOrder, 0, len(order))
	isSelected := s.IsSelectedColumn()
	for _, field := range order {
		name := s.ResolveColumnName(field.Column)
		var property *Property
		matches := 0
		if s.Schema() != nil {
			s.Schema().Properties.Range(func(key, value any) bool {
				if s.ResolveColumnName(key.(string)) == name {
					property = value.(*Property)
					matches++
				}
				return true
			})
		}
		var err error
		switch {
		case matches == 0:
			err = fmt.Errorf("sort column %q is absent from the source schema", field.Column)
		case matches > 1:
			err = fmt.Errorf("sort column %q has ambiguous destination name %q", field.Column, name)
		case !isSelected(name):
			err = fmt.Errorf("sort column %q is not selected", field.Column)
		case !sortPrimitive(property, inherited):
			requirement := "primitive"
			if inherited {
				requirement = "temporal primitive"
			}
			err = fmt.Errorf("sort column %q must be a supported %s", field.Column, requirement)
		}
		if err != nil {
			if inherited {
				return nil, nil
			}
			return nil, err
		}
		for _, previous := range resolved {
			if previous.Column == name {
				return nil, fmt.Errorf("duplicate destination sort column %q", name)
			}
		}
		field.Column = name
		resolved = append(resolved, field)
	}
	return resolved, nil
}

func sortPrimitive(property *Property, temporalOnly bool) bool {
	if property == nil || property.Type == nil || property.OlakeColumn {
		return false
	}
	var primitive DataType
	count := 0
	property.Type.Range(func(dataType DataType) {
		if dataType != Null {
			primitive = dataType
			count++
		}
	})
	if count != 1 {
		return false
	}
	switch primitive.ValueType() {
	case Date, TimestampNTZ, Timestamp, TimestampMilli, TimestampMicro, TimestampNano:
		return true
	case Bool, Int32, Int64, Float32, Float64, String, Binary:
		return !temporalOnly
	default:
		return !temporalOnly && BaseOf(primitive) == Decimal
	}
}
