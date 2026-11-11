package protocol

import (
	"fmt"
	"strings"

	"github.com/datazip-inc/olake/types"
)

type enrollmentSchema struct {
	Namespace           string `json:"namespace"`
	DestinationDatabase string `json:"destination_database"`
}

type enrollmentDefaults struct {
	Normalization  *bool            `json:"normalization"`
	AppendMode     *bool            `json:"append_mode"`
	UpdateType     types.UpdateType `json:"update_type"`
	SyncNewColumns *bool            `json:"sync_new_columns"`
	// Optional, unlike the others: policies saved before it existed leave it unset, which is off.
	DropRemovedColumns *bool `json:"drop_removed_columns"`
}

type enrollmentPolicy struct {
	Enabled            bool               `json:"enabled"`
	Schemas            []enrollmentSchema `json:"schemas"`
	Defaults           enrollmentDefaults `json:"defaults"`
	TargetQueryEngines []string           `json:"target_query_engines"`
}

func (p enrollmentPolicy) validate(saved *types.Catalog) error {
	if !p.Enabled {
		return nil
	}
	if p.Defaults.Normalization == nil || p.Defaults.AppendMode == nil || p.Defaults.SyncNewColumns == nil || p.Defaults.UpdateType == "" {
		return fmt.Errorf("enrollment defaults must explicitly set normalization, append_mode, update_type, and sync_new_columns")
	}
	if err := p.Defaults.UpdateType.Validate(); err != nil {
		return err
	}
	if len(p.Schemas) == 0 {
		return fmt.Errorf("enrollment requires at least one explicitly approved existing schema")
	}
	available := make(map[string]bool)
	for _, configured := range saved.Streams {
		if configured == nil || configured.Stream == nil {
			return fmt.Errorf("saved catalog contains an invalid stream; rediscover and save it before enabling enrollment")
		}
		available[configured.Namespace()] = true
	}
	seen := make(map[string]bool)
	for _, schema := range p.Schemas {
		if strings.TrimSpace(schema.Namespace) == "" || strings.TrimSpace(schema.DestinationDatabase) == "" {
			return fmt.Errorf("enrollment schema and destination_database must be nonblank")
		}
		if seen[schema.Namespace] {
			return fmt.Errorf("enrollment schema %q is approved more than once", schema.Namespace)
		}
		if !available[schema.Namespace] {
			return fmt.Errorf("enrollment schema %q is absent from the saved available catalog", schema.Namespace)
		}
		seen[schema.Namespace] = true
	}
	_, err := types.ParseQueryEngines(p.TargetQueryEngines)
	return err
}
