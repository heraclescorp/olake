package iceberg

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/datazip-inc/olake/types"
	"slices"
	"strings"
)

func canonicalResyncCheckpoint(checkpoint, expected string) ([]byte, error) {
	canonicalize := func(raw string) ([]byte, error) {
		if !json.Valid([]byte(raw)) {
			return nil, fmt.Errorf("published atomic resync has no valid full checkpoint")
		}
		var value any
		decoder := json.NewDecoder(strings.NewReader(raw))
		decoder.UseNumber()
		if err := decoder.Decode(&value); err != nil {
			return nil, err
		}
		object, ok := value.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("atomic resync requires a full checkpoint JSON object")
		}
		// Only known state collections are unordered. Cursor arrays retain their order.
		if global, ok := object["global"].(map[string]any); ok {
			if streams, ok := global["streams"].([]any); ok {
				names := make([]string, len(streams))
				for index, stream := range streams {
					name, ok := stream.(string)
					if !ok {
						return nil, fmt.Errorf("published checkpoint contains a non-string stream identity")
					}
					names[index] = name
				}
				slices.Sort(names)
				global["streams"] = names
			}
		}
		if streams, ok := object["streams"].([]any); ok {
			for _, stream := range streams {
				if record, ok := stream.(map[string]any); ok {
					if state, ok := record["state"].(map[string]any); ok {
						if chunks, ok := state[types.ChunksKey].([]any); ok {
							ordered, err := canonicalResyncRecords(chunks)
							if err != nil {
								return nil, err
							}
							state[types.ChunksKey] = ordered
						}
					}
				}
			}
			ordered, err := canonicalResyncRecords(streams)
			if err != nil {
				return nil, err
			}
			object["streams"] = ordered
		}
		return json.Marshal(value)
	}
	canonical, err := canonicalize(checkpoint)
	if err != nil {
		return nil, err
	}
	if expected != "" {
		wanted, err := canonicalize(expected)
		if err != nil {
			return nil, err
		}
		if !bytes.Equal(canonical, wanted) {
			return nil, fmt.Errorf("published atomic resync checkpoint differs from the final source checkpoint")
		}
	}
	return canonical, nil
}

// canonicalResyncRecords encodes each set member once, before comparing sort keys.
func canonicalResyncRecords(records []any) ([]json.RawMessage, error) {
	encoded := make([]json.RawMessage, len(records))
	for index, record := range records {
		value, err := json.Marshal(record)
		if err != nil {
			return nil, err
		}
		encoded[index] = value
	}
	slices.SortFunc(encoded, func(left, right json.RawMessage) int {
		return bytes.Compare(left, right)
	})
	return encoded, nil
}
