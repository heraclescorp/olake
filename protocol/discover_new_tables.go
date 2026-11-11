package protocol

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"
	"unicode"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/spf13/cobra"
)

var (
	enrollmentPolicyPath string
	enrollmentResultPath string
	enrollmentResultURI  string
)

type enrolledTable struct {
	Namespace  string `json:"namespace"`
	StreamName string `json:"stream_name"`
}

type skippedEnrollment struct {
	enrolledTable
	Reason string `json:"reason"`
}

type enrollmentResult struct {
	Enrolled               []enrolledTable     `json:"enrolled"`
	Skipped                []skippedEnrollment `json:"skipped"`
	StreamsConfig          string              `json:"streams_config"`
	AvailableStreamsConfig string              `json:"available_streams_config"`
	SelectedStreamsConfig  string              `json:"selected_streams_config"`
}

var discoverNewTablesCmd = &cobra.Command{
	Use:   "discover-new-tables",
	Short: "Propose approved new PostgreSQL tables without reading rows or starting a destination",
	Args:  cobra.NoArgs,
	PreRunE: func(_ *cobra.Command, _ []string) error {
		if err := validateCatalogFlags(true); err != nil {
			return err
		}
		if configPath == "" || configPath == "not-set" || !destinationPassed() || enrollmentPolicyPath == "" {
			return fmt.Errorf("discover-new-tables requires --config, --destination, and --enrollment-policy")
		}
		if noSave {
			return fmt.Errorf("discover-new-tables requires a persisted enrollment result; --no-save is unsupported")
		}
		output, err := filepath.Abs(enrollmentResultPath)
		if err != nil {
			return err
		}
		for _, input := range []string{configPath, destinationConfigPath, streamsPath, availableStreamsPath, selectedStreamsPath, statePath, enrollmentPolicyPath} {
			if input == "" {
				continue
			}
			path, err := filepath.Abs(input)
			if err != nil {
				return err
			}
			if path == output {
				return fmt.Errorf("enrollment result must not overwrite an input config/catalog/state file")
			}
		}
		return nil
	},
	RunE: func(cmd *cobra.Command, _ []string) error {
		saved, err := loadEnrollmentCatalog(streamsPath, availableStreamsPath, selectedStreamsPath)
		if err != nil {
			return err
		}
		var policy enrollmentPolicy
		if err := utils.UnmarshalFile(enrollmentPolicyPath, &policy, false); err != nil {
			return err
		}
		if err := policy.validate(saved.saved); err != nil {
			return err
		}
		var source abstract.TableEnrollmentSource
		if policy.Enabled {
			if connector.Type() != string(constants.Postgres) {
				return fmt.Errorf("automatic table enrollment is supported only for PostgreSQL")
			}
			if err := loadDestinationConfig(); err != nil {
				return err
			}
			if destinationConfig.Type != types.Iceberg {
				return fmt.Errorf("automatic table enrollment requires an Iceberg destination")
			}
			if err := utils.UnmarshalFile(configPath, connector.GetConfigRef(), true); err != nil {
				return err
			}
			var supported bool
			source, supported = connector.TableEnrollmentSource()
			if !supported {
				return fmt.Errorf("connector does not implement metadata-only table enrollment")
			}
			defer source.CloseConnection()
		}
		duration := constants.DefaultDiscoverTimeout
		if timeout != -1 {
			duration = time.Duration(timeout) * time.Second
		}
		ctx, cancel := context.WithTimeout(cmd.Context(), duration)
		defer cancel()
		result, err := discoverNewTables(ctx, source, saved, policy, destinationConfig, targetQueryEngines)
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		return writeEnrollmentResult(enrollmentResultPath, result)
	},
}

func init() {
	discoverNewTablesCmd.Flags().StringVar(&enrollmentPolicyPath, "enrollment-policy", "", "Explicit schema approvals and new-table defaults JSON")
	discoverNewTablesCmd.Flags().StringVar(&enrollmentResultPath, "enrollment-result", "", "Proposal result JSON (default: enrollment-result.json beside config)")
}

func discoverNewTables(ctx context.Context, source abstract.TableEnrollmentSource, saved *enrollmentCatalog, policy enrollmentPolicy, destination *types.WriterConfig, extraEngines []string) (*enrollmentResult, error) {
	result := &enrollmentResult{Enrolled: []enrolledTable{}, Skipped: []skippedEnrollment{}}
	if policy.Enabled {
		if err := policy.validate(saved.saved); err != nil {
			return nil, err
		}
		if destination == nil || destination.Type != types.Iceberg {
			return nil, fmt.Errorf("automatic table enrollment requires an Iceberg destination")
		}
		engines, err := types.ParseQueryEngines(append(slices.Clone(policy.TargetQueryEngines), extraEngines...))
		if err != nil {
			return nil, err
		}
		var writer enrollmentWriterConfig
		if err := utils.Unmarshal(destination.WriterConfig, &writer); err != nil {
			return nil, fmt.Errorf("invalid Iceberg writer configuration: %w", err)
		}
		s3Tables := writer.nativeS3Tables()
		var nativeDestination *nativeEnrollmentDestination
		known := make(map[types.StreamID]bool)
		knownCanonical := make(map[string]bool)
		occupied := make(map[string]bool)
		metadata := make(map[types.StreamID]types.StreamMetadata)
		for namespace, selections := range saved.saved.SelectedStreams {
			for _, selection := range selections {
				id := types.StreamID{Namespace: namespace, Name: selection.StreamName}
				metadata[id] = selection
				known[id] = true // a dangling exclusion/selection is not permission to replace it.
				knownCanonical[id.String()] = true
			}
		}
		for _, configured := range saved.saved.Streams {
			id := types.StreamID{Namespace: configured.Namespace(), Name: configured.Name()}
			known[id] = true
			knownCanonical[id.String()] = true
			copy := &types.ConfiguredStream{Stream: configured.Stream, StreamMetadata: metadata[id]}
			occupied[copy.GetDestinationDatabase(&writer.IcebergDatabase)+"\x00"+copy.GetDestinationTable()] = true
		}
		approved := make(map[string]string)
		for _, schema := range policy.Schemas {
			approved[schema.Namespace] = schema.DestinationDatabase
		}
		if err := source.SetupTableEnrollment(ctx); err != nil {
			return nil, err
		}
		names, err := source.GetStreamNames(ctx)
		if err != nil {
			return nil, err
		}
		slices.SortFunc(names, func(a, b types.StreamID) int {
			if order := strings.Compare(a.Namespace, b.Namespace); order != 0 {
				return order
			}
			return strings.Compare(a.Name, b.Name)
		})
		var candidates []types.StreamID
		targets := make(map[string]int)
		seen := make(map[types.StreamID]bool)
		for _, id := range names {
			if approved[id.Namespace] == "" || known[id] || seen[id] {
				continue
			}
			seen[id] = true
			candidates = append(candidates, id)
			targets[enrollmentTarget(approved[id.Namespace], id.Name)]++
		}
		var sourceSupported []types.UpdateType
		if restricter, ok := source.(abstract.UpdateTypeRestricter); ok {
			sourceSupported = restricter.SupportedUpdateTypes()
		}
		available, err := types.AvailableUpdateTypes(engines, destination, sourceSupported)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve update types for enrolled tables: %w", err)
		}
		for _, id := range candidates {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			identity := enrolledTable{Namespace: id.Namespace, StreamName: id.Name}
			skip := func(reason string) {
				result.Skipped = append(result.Skipped, skippedEnrollment{enrolledTable: identity, Reason: reason})
				logger.Infof("Automatic enrollment skipped %s: %s", id.String(), reason)
			}
			if !safeEnrollmentSourceName(id.Namespace) || !safeEnrollmentSourceName(id.Name) || knownCanonical[id.String()] {
				skip("source identity contains unsupported characters or is ambiguous with a saved stream")
				continue
			}
			target := enrollmentTarget(approved[id.Namespace], id.Name)
			_, table := utils.GenerateDestinationDetails(id.Namespace, id.Name, nil)
			database := utils.Reformat(approved[id.Namespace])
			if table == "" || database == "" || (s3Tables && (!validS3TableName(table) || !validS3Namespace(database))) {
				skip("canonical destination name is unsafe or violates S3 Tables naming rules")
				continue
			}
			if occupied[target] || targets[target] > 1 {
				skip("canonical destination collides with a saved stream or another new candidate")
				continue
			}
			if !*policy.Defaults.AppendMode && (len(available) == 0 || !slices.Contains(available, policy.Defaults.UpdateType)) {
				skip("policy delete format is not readable by every target query engine")
				continue
			}
			if !*policy.Defaults.AppendMode && policy.Defaults.UpdateType == types.UpdateTypeEquality && (writer.NoIdentifierFields || writer.CatalogType == "unity") {
				skip("destination does not support identifier fields required by equality deletes")
				continue
			}
			stream, err := source.ProduceSchema(ctx, id)
			if err != nil || stream == nil {
				skip("table schema or primary-key metadata is unavailable")
				continue
			}
			if stream.Namespace != id.Namespace || stream.Name != id.Name {
				skip("discovered schema does not match the source identity")
				continue
			}
			stream.DestinationDatabase = approved[id.Namespace]
			stream.DestinationTable = table
			stream.DefaultStreamProperties = &types.DefaultStreamProperties{Normalization: *policy.Defaults.Normalization, AppendMode: *policy.Defaults.AppendMode, UpdateType: policy.Defaults.UpdateType}
			stream.AvailableUpdateTypes = available
			if reason := enrollmentSchemaIssue(stream, *policy.Defaults.AppendMode); reason != "" {
				skip(reason)
				continue
			}
			if err := source.ValidateTableEnrollment(ctx, stream); err != nil {
				skip(err.Error())
				continue
			}
			if s3Tables {
				if nativeDestination == nil {
					nativeDestination = newNativeEnrollmentDestination(ctx, writer)
				}
				if reason := nativeDestination.skipReason(ctx, database, table); reason != "" {
					skip(reason)
					continue
				}
			}
			for column, datatype := range abstract.DefaultColumns {
				stream.UpsertField(column, datatype, true, true)
			}
			stream.SyncMode = types.CDC // ordinary CDC sync snapshots a stream with no prior state.
			stream.RefreshSelectableColumns()
			if err := saved.add(stream, policy.Defaults); err != nil {
				return nil, err
			}
			occupied[target] = true
			result.Enrolled = append(result.Enrolled, identity)
			logger.Infof("Automatic enrollment proposed %s", id.String())
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var err error
	result.StreamsConfig, result.AvailableStreamsConfig, result.SelectedStreamsConfig, err = saved.result(len(result.Enrolled) > 0)
	return result, err
}

func enrollmentTarget(database, sourceTable string) string {
	_, table := utils.GenerateDestinationDetails("", sourceTable, nil)
	return utils.Reformat(database) + "\x00" + table
}

var s3TableName = regexp.MustCompile(`^[a-z0-9][a-z0-9_]{0,254}$`)

// https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-buckets-naming.html
// https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-namespace-create.html
func validS3TableName(name string) bool { return s3TableName.MatchString(name) }
func validS3Namespace(name string) bool {
	return validS3TableName(name) && !strings.HasPrefix(name, "aws") && !strings.HasSuffix(name, "_")
}

func safeEnrollmentSourceName(name string) bool {
	return strings.TrimSpace(name) != "" && !strings.Contains(name, ".") && !strings.ContainsFunc(name, unicode.IsControl)
}

func enrollmentSchemaIssue(stream *types.Stream, appendMode bool) string {
	if stream.Schema == nil || len(stream.Schema.ColumnNames()) == 0 {
		return "table schema is empty or unavailable"
	}
	if stream.SupportedSyncModes == nil || !stream.SupportedSyncModes.Exists(types.CDC) {
		return "table does not support CDC with an ordinary initial snapshot"
	}
	columns := make(map[string]string)
	for _, column := range stream.Schema.ColumnNames() {
		normalized := utils.Reformat(column)
		if previous, exists := columns[normalized]; exists && previous != column {
			return "source columns collide after canonical destination naming"
		}
		if _, reserved := abstract.DefaultColumns[normalized]; reserved {
			_, property := stream.Schema.GetProperty(column)
			if !property.OlakeColumn {
				return "source column collides with a reserved OLake metadata column"
			}
		}
		if normalized == "" {
			return "source column has an empty canonical destination name"
		}
		columns[normalized] = column
	}
	if !appendMode {
		if stream.SourceDefinedPrimaryKey == nil || stream.SourceDefinedPrimaryKey.Len() == 0 {
			return "upsert enrollment requires a stable source primary key"
		}
		for _, key := range stream.SourceDefinedPrimaryKey.Array() {
			found, property := stream.Schema.GetProperty(key)
			if !found || property.Nullable() {
				return "source primary-key schema is missing or nullable"
			}
		}
	}
	return ""
}

// Atomically publish the single output artifact only after the entire metadata
// scan succeeds. Neither input catalogs nor state are written by this command.
func writeEnrollmentResult(path string, result *enrollmentResult) error {
	data, err := json.Marshal(result)
	if err != nil {
		return err
	}
	file, err := os.CreateTemp(filepath.Dir(path), ".enrollment-result-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	if _, err := file.Write(data); err != nil {
		file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return os.Rename(file.Name(), path)
}
