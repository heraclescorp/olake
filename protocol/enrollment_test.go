package protocol

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/datazip-inc/olake/types"
)

type enrollmentTestSource struct {
	names    []types.StreamID
	produced []types.StreamID
	invalid  map[string]string
	noPK     map[string]bool
	setup    int
}

func (s *enrollmentTestSource) SetupTableEnrollment(context.Context) error { s.setup++; return nil }
func (s *enrollmentTestSource) CloseConnection()                           {}
func (s *enrollmentTestSource) GetStreamNames(context.Context) ([]types.StreamID, error) {
	return slices.Clone(s.names), nil
}
func (s *enrollmentTestSource) ProduceSchema(_ context.Context, id types.StreamID) (*types.Stream, error) {
	s.produced = append(s.produced, id)
	stream := types.NewStream(id.Name, id.Namespace, nil)
	stream.UpsertField("id", types.Int64, false, false)
	stream.UpsertField("payload", types.String, true, false)
	stream.WithSyncMode(types.CDC, types.FULLREFRESH, types.INCREMENTAL)
	if !s.noPK[id.Name] {
		stream.WithPrimaryKey("id")
	}
	return stream, nil
}
func (s *enrollmentTestSource) ValidateTableEnrollment(_ context.Context, stream *types.Stream) error {
	if reason := s.invalid[stream.Name]; reason != "" {
		return errors.New(reason)
	}
	return nil
}

const enrollmentSavedFixture = `{
 "future_root":{"keep":true},
 "streams":[
  {"future_stream":7,"stream":{"name":"kept","namespace":"public","sync_mode":"cdc","destination_database":"warehouse:public","destination_table":"kept","default_stream_properties":{"normalization":false,"append_mode":true,"update_type":"eq"},"type_schema":{"properties":{"old":{"type":["string"]}}}}},
  {"stream":{"name":"excluded","namespace":"public","sync_mode":"cdc","destination_database":"warehouse:public","destination_table":"excluded"}}
 ],
 "selected_streams":{"public":[{"stream_name":"kept","normalization":false,"append_mode":true,"update_type":"eq","selected_columns":{"columns":["old"],"sync_new_columns":false},"future_override":{"exact":123}}]}
}`

func enrollmentTestCatalog(t *testing.T, split bool, input string) *enrollmentCatalog {
	t.Helper()
	dir := t.TempDir()
	legacyPath := filepath.Join(dir, "streams.json")
	if err := os.WriteFile(legacyPath, []byte(input), 0600); err != nil {
		t.Fatal(err)
	}
	if !split {
		catalog, err := loadEnrollmentCatalog(legacyPath, "", "")
		if err != nil {
			t.Fatal(err)
		}
		return catalog
	}
	var document map[string]json.RawMessage
	if err := json.Unmarshal([]byte(input), &document); err != nil {
		t.Fatal(err)
	}
	selected := map[string]json.RawMessage{"selected_streams": document["selected_streams"], "future_selection_root": json.RawMessage(`true`)}
	delete(document, "selected_streams")
	availableBytes, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	selectedBytes, err := json.Marshal(selected)
	if err != nil {
		t.Fatal(err)
	}
	availablePath, selectedPath := filepath.Join(dir, "available_streams.json"), filepath.Join(dir, "selected_streams.json")
	if err := os.WriteFile(availablePath, availableBytes, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(selectedPath, selectedBytes, 0600); err != nil {
		t.Fatal(err)
	}
	catalog, err := loadEnrollmentCatalog("", availablePath, selectedPath)
	if err != nil {
		t.Fatal(err)
	}
	return catalog
}

func enrollmentTestPolicy() enrollmentPolicy {
	return enrollmentPolicy{
		Enabled:            true,
		Schemas:            []enrollmentSchema{{Namespace: "public", DestinationDatabase: "warehouse:public"}},
		Defaults:           enrollmentDefaults{Normalization: new(true), AppendMode: new(false), UpdateType: types.UpdateTypePosition, SyncNewColumns: new(false)},
		TargetQueryEngines: []string{"athena"},
	}
}

func enrollmentTestDestination() *types.WriterConfig {
	return &types.WriterConfig{Type: types.Iceberg, WriterConfig: map[string]any{"catalog_type": "rest", "rest_catalog_url": "http://iceberg-fixture.invalid", "iceberg_s3_path": "s3://enrollment-smoke/warehouse"}}
}

func TestEnrollmentExplicitPolicy(t *testing.T) {
	saved := enrollmentTestCatalog(t, false, enrollmentSavedFixture)
	cases := []struct {
		name   string
		change func(*enrollmentPolicy)
		want   string
	}{
		{"missing defaults", func(p *enrollmentPolicy) { p.Defaults.Normalization = nil }, "explicitly"},
		{"missing new-column policy", func(p *enrollmentPolicy) { p.Defaults.SyncNewColumns = nil }, "explicitly"},
		{"no schemas", func(p *enrollmentPolicy) { p.Schemas = nil }, "approved"},
		{"new schema", func(p *enrollmentPolicy) { p.Schemas[0].Namespace = "unapproved" }, "absent"},
		{"duplicate schema", func(p *enrollmentPolicy) { p.Schemas = append(p.Schemas, p.Schemas[0]) }, "more than once"},
		{"blank destination", func(p *enrollmentPolicy) { p.Schemas[0].DestinationDatabase = " " }, "nonblank"},
		{"unknown delete type", func(p *enrollmentPolicy) { p.Defaults.UpdateType = "future" }, "invalid"},
		{"unknown reader", func(p *enrollmentPolicy) { p.TargetQueryEngines = []string{"typo"} }, "unknown"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			policy := enrollmentTestPolicy()
			tc.change(&policy)
			if err := policy.validate(saved.saved); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("wanted %q, got %v", tc.want, err)
			}
		})
	}
	if err := enrollmentTestPolicy().validate(saved.saved); err != nil {
		t.Fatal(err)
	}
}

func TestEnrollmentApprovedSubsetPreservesBothCatalogFormats(t *testing.T) {
	for _, split := range []bool{false, true} {
		t.Run(map[bool]string{false: "legacy", true: "split"}[split], func(t *testing.T) {
			saved := enrollmentTestCatalog(t, split, enrollmentSavedFixture)
			oldStreams := slices.Clone(saved.streams)
			oldSelection := slices.Clone(saved.selections["public"])
			source := &enrollmentTestSource{
				names: []types.StreamID{{Namespace: "public", Name: "valid"}, {Namespace: "public", Name: "kept"}, {Namespace: "outside", Name: "not_approved"}, {Namespace: "public", Name: "excluded"}, {Namespace: "public", Name: "no_pk"}, {Namespace: "public", Name: "unpublished"}, {Namespace: "public", Name: "invalid.name"}},
				noPK:  map[string]bool{"no_pk": true}, invalid: map[string]string{"unpublished": "table is not covered by the configured publication"},
			}
			result, err := discoverNewTables(context.Background(), source, saved, enrollmentTestPolicy(), enrollmentTestDestination(), nil)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(result.Enrolled, []enrolledTable{{Namespace: "public", StreamName: "valid"}}) {
				t.Fatalf("unexpected enrollment: %+v", result.Enrolled)
			}
			if !reflect.DeepEqual(source.produced, []types.StreamID{{Namespace: "public", Name: "no_pk"}, {Namespace: "public", Name: "unpublished"}, {Namespace: "public", Name: "valid"}}) {
				t.Fatalf("schema fetched outside valid name/approval subset: %+v", source.produced)
			}
			if len(result.Skipped) != 3 {
				t.Fatalf("unexpected skips: %+v", result.Skipped)
			}
			for i := range oldStreams {
				assertEnrollmentJSONEqual(t, oldStreams[i], saved.streams[i])
			}
			assertEnrollmentJSONEqual(t, oldSelection[0], saved.selections["public"][0])
			if len(saved.selections["public"]) != 2 {
				t.Fatal("known excluded stream was selected")
			}
			if split {
				if result.StreamsConfig != "" || result.AvailableStreamsConfig == "" || result.SelectedStreamsConfig == "" {
					t.Fatal("wrong output format")
				}
			} else if result.StreamsConfig == "" || result.AvailableStreamsConfig != "" || result.SelectedStreamsConfig != "" {
				t.Fatal("wrong output format")
			}
			var metadata map[string]json.RawMessage
			if err := json.Unmarshal(saved.selections["public"][1], &metadata); err != nil {
				t.Fatal(err)
			}
			if string(metadata["append_mode"]) != "false" {
				t.Fatal("new append default must be explicit, including false")
			}
			var columns types.SelectedColumns
			if err := json.Unmarshal(metadata["selected_columns"], &columns); err != nil {
				t.Fatal(err)
			}
			if columns.SyncNewColumns || !slices.Contains(columns.Columns, "payload") || !slices.Contains(columns.Columns, "_olake_id") {
				t.Fatalf("new-column policy was not explicit: %+v", columns)
			}
			// Re-load the exact proposed result through the ordinary reader: no custom runtime format.
			dir := t.TempDir()
			var loaded *types.Catalog
			if split {
				a, s := filepath.Join(dir, "available.json"), filepath.Join(dir, "selected.json")
				if err := os.WriteFile(a, []byte(result.AvailableStreamsConfig), 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(s, []byte(result.SelectedStreamsConfig), 0600); err != nil {
					t.Fatal(err)
				}
				loaded, err = types.ResolveCatalog("", a, s)
			} else {
				path := filepath.Join(dir, "streams.json")
				if err := os.WriteFile(path, []byte(result.StreamsConfig), 0600); err != nil {
					t.Fatal(err)
				}
				loaded, err = types.ResolveCatalog(path, "", "")
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(loaded.Streams) != 3 || len(loaded.SelectedStreams["public"]) != 2 {
				t.Fatal("ordinary reader lost selection or definitions")
			}
			selection := loaded.SelectedStreams["public"][1]
			stream := &types.ConfiguredStream{Stream: loaded.Streams[2].Stream, StreamMetadata: selection}
			if stream.GetSyncMode() != types.CDC || stream.GetDestinationDatabase(nil) != "warehouse_public" || stream.GetDestinationTable() != "valid" {
				t.Fatal("ordinary reader resolved different destination or sync mode")
			}
			if stream.GetUpdateType() != types.UpdateTypePosition || stream.AppendModeEnabled() || !stream.NormalizationEnabled() {
				t.Fatal("ordinary reader lost explicit defaults")
			}
		})
	}
}

func assertEnrollmentJSONEqual(t *testing.T, before, after []byte) {
	t.Helper()
	var a, b any
	if err := json.Unmarshal(before, &a); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(after, &b); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(a, b) {
		t.Fatalf("existing JSON changed: %s => %s", before, after)
	}
}

func TestEnrollmentLegacyImplicitSelectionPreservesEffectiveDefaults(t *testing.T) {
	var document map[string]json.RawMessage
	if err := json.Unmarshal([]byte(enrollmentSavedFixture), &document); err != nil {
		t.Fatal(err)
	}
	delete(document, "selected_streams")
	data, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	saved := enrollmentTestCatalog(t, false, string(data))
	oldStreams := slices.Clone(saved.streams)
	result, err := discoverNewTables(context.Background(), &enrollmentTestSource{names: []types.StreamID{{Namespace: "public", Name: "new_table"}}}, saved, enrollmentTestPolicy(), enrollmentTestDestination(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Enrolled) != 1 || len(saved.selections["public"]) != 3 {
		t.Fatal("implicit old selection was lost")
	}
	for i := range oldStreams {
		assertEnrollmentJSONEqual(t, oldStreams[i], saved.streams[i])
	}
	for i, raw := range saved.selections["public"][:2] {
		var old types.LegacyStreamMetadata
		if err := json.Unmarshal(raw, &old); err != nil {
			t.Fatal(err)
		}
		if old.SelectedColumns != nil {
			t.Fatal("implicit old all/future-column policy must remain null, not inherit the new-table false policy")
		}
		if i == 0 && (old.Normalization || !old.AppendMode || old.UpdateType != "eq") {
			t.Fatalf("old effective defaults changed: %+v", old)
		}
		if i == 1 && (old.Normalization || old.AppendMode || old.UpdateType != "eq") {
			t.Fatalf("old absent defaults changed: %+v", old)
		}
	}
	var added types.LegacyStreamMetadata
	if err := json.Unmarshal(saved.selections["public"][2], &added); err != nil {
		t.Fatal(err)
	}
	if added.SelectedColumns == nil || added.SelectedColumns.SyncNewColumns {
		t.Fatal("explicit new columns=false was lost")
	}
}

func TestEnrollmentCollisionAndReaderRejectionBeforeSchema(t *testing.T) {
	for _, tc := range []struct {
		name   string
		names  []types.StreamID
		change func(*enrollmentPolicy)
	}{
		{"candidate collision", []types.StreamID{{Namespace: "public", Name: "new-table"}, {Namespace: "public", Name: "new_table"}}, func(*enrollmentPolicy) {}},
		{"known destination collision", []types.StreamID{{Namespace: "public", Name: "KEPT"}}, func(*enrollmentPolicy) {}},
		{"reader format", []types.StreamID{{Namespace: "public", Name: "valid"}}, func(p *enrollmentPolicy) { p.TargetQueryEngines = []string{"databricks"} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			saved := enrollmentTestCatalog(t, false, enrollmentSavedFixture)
			source := &enrollmentTestSource{names: tc.names}
			policy := enrollmentTestPolicy()
			tc.change(&policy)
			result, err := discoverNewTables(context.Background(), source, saved, policy, enrollmentTestDestination(), nil)
			if err != nil {
				t.Fatal(err)
			}
			if len(source.produced) != 0 || len(result.Enrolled) != 0 || len(result.Skipped) != len(tc.names) {
				t.Fatalf("unsafe candidate reached schema discovery: %+v %+v", source.produced, result)
			}
			if result.StreamsConfig != saved.originalAvailable {
				t.Fatal("no-enrollment output must be byte-identical")
			}
		})
	}
}

func TestEnrollmentDisabledAndCancelled(t *testing.T) {
	saved := enrollmentTestCatalog(t, false, enrollmentSavedFixture)
	source := &enrollmentTestSource{}
	result, err := discoverNewTables(context.Background(), source, saved, enrollmentPolicy{}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if source.setup != 0 || result.StreamsConfig != enrollmentSavedFixture {
		t.Fatal("disabled policy touched source or catalog")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := discoverNewTables(ctx, source, saved, enrollmentPolicy{}, nil, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled proposal must not succeed: %v", err)
	}
}

func TestEnrollmentS3NamesAndPrimaryKeyEligibility(t *testing.T) {
	for _, tc := range []struct {
		name      string
		namespace bool
		want      bool
	}{
		{"valid_table", false, true}, {"_invalid", false, false}, {"a", false, true}, {"Mixed", false, false}, {strings.Repeat("a", 256), false, false}, {"aws_reserved", true, false}, {"trailing_", true, false}, {"warehouse_public", true, true},
	} {
		got := validS3TableName(tc.name)
		if tc.namespace {
			got = validS3Namespace(tc.name)
		}
		if got != tc.want {
			t.Fatalf("name %q namespace=%v: got %v", tc.name, tc.namespace, got)
		}
	}
	stream := types.NewStream("candidate", "public", nil).WithSyncMode(types.CDC)
	stream.UpsertField("id", types.Int64, false, false)
	if !strings.Contains(enrollmentSchemaIssue(stream, false), "primary key") {
		t.Fatal("upsert without PK accepted")
	}
	if issue := enrollmentSchemaIssue(stream, true); issue != "" {
		t.Fatalf("append should not require PK: %s", issue)
	}
	stream.WithPrimaryKey("id")
	stream.UpsertField("a-b", types.String, true, false)
	stream.UpsertField("a_b", types.String, true, false)
	if !strings.Contains(enrollmentSchemaIssue(stream, false), "collide") {
		t.Fatal("column naming collision accepted")
	}
}

func TestEnrollmentArtifactContainsCompleteProposal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "enrollment-result.json")
	result := &enrollmentResult{Enrolled: []enrolledTable{}, Skipped: []skippedEnrollment{}, StreamsConfig: enrollmentSavedFixture}
	if err := writeEnrollmentResult(path, result); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var loaded enrollmentResult
	if err := json.Unmarshal(data, &loaded); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(result, &loaded) {
		t.Fatal("result artifact differs from proposal")
	}
}

func TestEnrollmentDeterministicProposalAndPersistedRetry(t *testing.T) {
	names := []types.StreamID{{Namespace: "public", Name: "z_new"}, {Namespace: "public", Name: "a_new"}, {Namespace: "public", Name: "a_new"}}
	var first *enrollmentResult
	for range 2 {
		saved := enrollmentTestCatalog(t, false, enrollmentSavedFixture)
		result, err := discoverNewTables(context.Background(), &enrollmentTestSource{names: names}, saved, enrollmentTestPolicy(), enrollmentTestDestination(), nil)
		if err != nil {
			t.Fatal(err)
		}
		if first == nil {
			first = result
		} else if !reflect.DeepEqual(first, result) {
			t.Fatal("identical source/policy/catalog must produce deterministic output")
		}
	}
	persisted := enrollmentTestCatalog(t, false, first.StreamsConfig)
	source := &enrollmentTestSource{names: names}
	retry, err := discoverNewTables(context.Background(), source, persisted, enrollmentTestPolicy(), enrollmentTestDestination(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(retry.Enrolled) != 0 || len(source.produced) != 0 || retry.StreamsConfig != first.StreamsConfig {
		t.Fatal("retry after worker persistence must neither refetch schemas nor add duplicates")
	}
}

func TestEnrollmentAppendPolicyDoesNotRequireDeleteReaderOrPrimaryKey(t *testing.T) {
	policy := enrollmentTestPolicy()
	policy.Defaults.AppendMode = new(true)
	policy.Defaults.UpdateType = types.UpdateTypeEquality
	policy.TargetQueryEngines = []string{"databricks"}
	source := &enrollmentTestSource{names: []types.StreamID{{Namespace: "public", Name: "append_table"}}, noPK: map[string]bool{"append_table": true}}
	result, err := discoverNewTables(context.Background(), source, enrollmentTestCatalog(t, false, enrollmentSavedFixture), policy, enrollmentTestDestination(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Enrolled) != 1 {
		t.Fatalf("append-only policy should not require PK/delete compatibility: %+v", result.Skipped)
	}
}

func TestEnrollmentBlockedCandidatesRemainRetryable(t *testing.T) {
	names := []types.StreamID{{Namespace: "public", Name: "no_pk_yet"}, {Namespace: "public", Name: "not_published_yet"}}
	saved := enrollmentTestCatalog(t, false, enrollmentSavedFixture)
	blocked := &enrollmentTestSource{names: names, noPK: map[string]bool{"no_pk_yet": true}, invalid: map[string]string{"not_published_yet": "table is not covered by the configured publication"}}
	first, err := discoverNewTables(context.Background(), blocked, saved, enrollmentTestPolicy(), enrollmentTestDestination(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Enrolled) != 0 || len(first.Skipped) != 2 || first.StreamsConfig != enrollmentSavedFixture {
		t.Fatal("blocked candidates must not be persisted as known exclusions")
	}
	repaired := &enrollmentTestSource{names: names}
	second, err := discoverNewTables(context.Background(), repaired, saved, enrollmentTestPolicy(), enrollmentTestDestination(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Enrolled) != 2 || len(repaired.produced) != 2 {
		t.Fatal("fixed PK/publication must be reconsidered on the next metadata preflight")
	}
}
