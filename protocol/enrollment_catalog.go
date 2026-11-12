package protocol

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/datazip-inc/olake/types"
)

// Old JSON subtrees remain raw: typed discovery/merge intentionally rewrites
// settings and is therefore not appropriate for an enrollment-only proposal.
type enrollmentCatalog struct {
	saved             *types.Catalog
	legacy            bool
	available         map[string]json.RawMessage
	selected          map[string]json.RawMessage
	streams           []json.RawMessage
	selections        map[string][]json.RawMessage
	originalAvailable string
	originalSelected  string
	implicit          bool
}

func readEnrollmentDocument(path string) (map[string]json.RawMessage, string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, "", err
	}
	var document map[string]json.RawMessage
	if err := json.Unmarshal(data, &document); err != nil {
		return nil, "", err
	}
	if document == nil {
		return nil, "", fmt.Errorf("catalog must be a JSON object")
	}
	return document, string(data), nil
}

func loadEnrollmentCatalog(legacyPath, availablePath, selectedPath string) (*enrollmentCatalog, error) {
	saved, err := types.ResolveCatalog(legacyPath, availablePath, selectedPath)
	if err != nil {
		return nil, err
	}
	c := &enrollmentCatalog{saved: saved, legacy: legacyPath != ""}
	path := availablePath
	if c.legacy {
		path = legacyPath
	}
	c.available, c.originalAvailable, err = readEnrollmentDocument(path)
	if err != nil {
		return nil, err
	}
	c.selected = c.available
	if !c.legacy {
		c.selected, c.originalSelected, err = readEnrollmentDocument(selectedPath)
		if err != nil {
			return nil, err
		}
	}
	if raw, ok := c.available["streams"]; ok {
		if err := json.Unmarshal(raw, &c.streams); err != nil {
			return nil, err
		}
	}
	if raw, ok := c.selected["selected_streams"]; ok {
		if err := json.Unmarshal(raw, &c.selections); err != nil {
			return nil, err
		}
	}
	c.implicit = c.legacy && c.selections == nil
	if c.selections == nil {
		c.selections = make(map[string][]json.RawMessage)
	}
	return c, nil
}

// A missing legacy selection means all saved streams with zero metadata. Spell
// out those effective values only when adding the first table. Null selected_columns
// retains the exact old "all current and future columns" behavior; policy defaults
// apply only to new streams, never to these materialized selections.
func (c *enrollmentCatalog) materializeImplicitSelection() error {
	if !c.implicit {
		return nil
	}
	for _, configured := range c.saved.Streams {
		old := &types.ConfiguredStream{Stream: configured.Stream}
		metadata := types.LegacyStreamMetadata{
			StreamName:    old.Name(),
			Normalization: old.NormalizationEnabled(),
			AppendMode:    old.AppendModeEnabled(),
			UpdateType:    string(old.GetUpdateType()),
		}
		raw, err := json.Marshal(metadata)
		if err != nil {
			return err
		}
		c.selections[old.Namespace()] = append(c.selections[old.Namespace()], raw)
	}
	c.implicit = false
	return nil
}

func (c *enrollmentCatalog) add(stream *types.Stream, defaults enrollmentDefaults) error {
	if err := c.materializeImplicitSelection(); err != nil {
		return err
	}
	definition, err := json.Marshal(&types.ConfiguredStream{Stream: stream})
	if err != nil {
		return err
	}
	columns := &types.SelectedColumns{Columns: stream.Schema.ColumnNames(), SyncNewColumns: *defaults.SyncNewColumns}
	dropRemovedColumns := defaults.DropRemovedColumns != nil && *defaults.DropRemovedColumns
	var metadata any = types.StreamMetadata{
		StreamName:          stream.Name,
		Normalization:       defaults.Normalization,
		AppendMode:          defaults.AppendMode,
		UpdateType:          new(string(defaults.UpdateType)),
		DropRemovedColumns:  dropRemovedColumns,
		SelectedColumns:     columns,
		SyncMode:            stream.SyncMode,
		DestinationDatabase: stream.DestinationDatabase,
		DestinationTable:    stream.DestinationTable,
	}
	if c.legacy {
		metadata = struct {
			types.LegacyStreamMetadata
			AppendMode bool `json:"append_mode"`
		}{
			LegacyStreamMetadata: types.LegacyStreamMetadata{
				StreamName:         stream.Name,
				Normalization:      *defaults.Normalization,
				UpdateType:         string(defaults.UpdateType),
				DropRemovedColumns: dropRemovedColumns,
				SelectedColumns:    columns,
			},
			AppendMode: *defaults.AppendMode,
		}
	}
	raw, err := json.Marshal(metadata)
	if err != nil {
		return err
	}
	c.streams = append(c.streams, definition)
	c.selections[stream.Namespace] = append(c.selections[stream.Namespace], raw)
	return nil
}

func (c *enrollmentCatalog) result(changed bool) (string, string, string, error) {
	available, selected := c.originalAvailable, c.originalSelected
	if changed {
		definitions, err := json.Marshal(c.streams)
		if err != nil {
			return "", "", "", err
		}
		selections, err := json.Marshal(c.selections)
		if err != nil {
			return "", "", "", err
		}
		c.available["streams"] = definitions
		c.selected["selected_streams"] = selections
		data, err := json.Marshal(c.available)
		if err != nil {
			return "", "", "", err
		}
		available = string(data)
		if !c.legacy {
			data, err = json.Marshal(c.selected)
			if err != nil {
				return "", "", "", err
			}
			selected = string(data)
		}
	}
	if c.legacy {
		return available, "", "", nil
	}
	return "", available, selected, nil
}
