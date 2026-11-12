package iceberg

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/datazip-inc/olake/destination/iceberg/proto"
)

func TestJavaHeapEnvironment(t *testing.T) {
	for _, test := range []struct {
		name string
		env  []string
		want []string
	}{
		{"absent", []string{"_JAVA_OPTIONS=-XX:MaxRAMPercentage=75 -Dkeep=yes", "OTHER=value"}, []string{"_JAVA_OPTIONS=-XX:MaxRAMPercentage=75 -Dkeep=yes", "OTHER=value"}},
		{"append", []string{"OLAKE_JAVA_HEAP_PERCENT=50", "OTHER=value"}, []string{"OLAKE_JAVA_HEAP_PERCENT=50", "OTHER=value", "_JAVA_OPTIONS=-XX:MaxRAMPercentage=50"}},
		{"override inherited", []string{"_JAVA_OPTIONS=-XX:MaxRAMPercentage=75 -Dkeep=yes", "OLAKE_JAVA_HEAP_PERCENT=40"}, []string{"_JAVA_OPTIONS=-XX:MaxRAMPercentage=75 -Dkeep=yes -XX:MaxRAMPercentage=40", "OLAKE_JAVA_HEAP_PERCENT=40"}},
		{"empty options", []string{"_JAVA_OPTIONS=", "OLAKE_JAVA_HEAP_PERCENT=1"}, []string{"_JAVA_OPTIONS=-XX:MaxRAMPercentage=1", "OLAKE_JAVA_HEAP_PERCENT=1"}},
		{"upper bound", []string{"OLAKE_JAVA_HEAP_PERCENT=80"}, []string{"OLAKE_JAVA_HEAP_PERCENT=80", "_JAVA_OPTIONS=-XX:MaxRAMPercentage=80"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			original := append([]string(nil), test.env...)
			got, err := javaHeapEnvironment(test.env)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(test.want, got) {
				t.Fatalf("want %v, got %v", test.want, got)
			}
			if !reflect.DeepEqual(original, test.env) {
				t.Fatal("mutated parent environment")
			}
		})
	}
}

func TestJavaHeapEnvironmentRejectsInvalidPolicy(t *testing.T) {
	for _, value := range []string{"", "0", "-1", "81", "50.5", "NaN", "Inf", "1e1", " 50", "secret-value"} {
		t.Run(value, func(t *testing.T) {
			_, err := javaHeapEnvironment([]string{"OLAKE_JAVA_HEAP_PERCENT=" + value})
			if err == nil {
				t.Fatal("expected invalid policy error")
			}
			if strings.Contains(err.Error(), "secret-value") {
				t.Fatal("error leaked invalid environment value")
			}
		})
	}
}

func TestJavaHeapPolicyRejectsOptionsThatBypassContainerBudget(t *testing.T) {
	for _, key := range []string{"_JAVA_OPTIONS", "JAVA_TOOL_OPTIONS", "JDK_JAVA_OPTIONS"} {
		for _, option := range []string{"-Xmx1g", "-XX:MaxHeapSize=1073741824", "-XX:MaxRAM=1g", "-XX:-UseContainerSupport"} {
			_, err := javaHeapEnvironment([]string{"OLAKE_JAVA_HEAP_PERCENT=50", key + "=" + option})
			if err == nil {
				t.Fatalf("accepted %s bypassing the per-job budget", key)
			}
		}
	}
}

func TestAtomicResyncStartupPolicyIsInternalAndResyncOnly(t *testing.T) {
	for _, resync := range []bool{false, true} {
		config := &Config{CatalogType: RestCatalog, atomicResync: resync}
		encoded, err := getServerConfigJSON(config, defaultServerPort, true, "")
		if err != nil {
			t.Fatal(err)
		}
		var startup map[string]string
		if err := json.Unmarshal(encoded, &startup); err != nil {
			t.Fatal(err)
		}
		marker, exists := startup["atomic-resync-enabled"]
		if exists != resync || (resync && marker != "true") {
			t.Fatalf("resync=%t: unexpected startup policy marker %q, present=%t", resync, marker, exists)
		}
	}
	var userConfig Config
	if err := json.Unmarshal([]byte(`{"atomicResync":true,"atomic-resync-enabled":true}`), &userConfig); err != nil {
		t.Fatal(err)
	}
	if userConfig.atomicResync {
		t.Fatal("ordinary destination JSON must not enable the internal resync startup policy")
	}
}

func TestControlledRuntimeStartupFenceAndRecoveryIdentity(t *testing.T) {
	config := &Config{CatalogType: RestCatalog, atomicResync: true,
		executionID: "recovery-runtime", executionFence: "s3://files/runtime/execution-fence.json",
		resyncLaunchID: "original-launch", resyncRecoverOnly: true}
	encoded, err := getServerConfigJSON(config, defaultServerPort, false, "")
	if err != nil {
		t.Fatal(err)
	}
	var startup map[string]string
	if err := json.Unmarshal(encoded, &startup); err != nil {
		t.Fatal(err)
	}
	for key, want := range map[string]string{
		"execution-id": "recovery-runtime", "execution-fence": config.executionFence,
		"resync-launch-id": "original-launch", "resync-recover-only": "true",
	} {
		if startup[key] != want {
			t.Fatalf("%s: got %q, want %q", key, startup[key], want)
		}
	}
	standalone, err := getServerConfigJSON(&Config{CatalogType: RestCatalog}, defaultServerPort, false, "")
	if err != nil {
		t.Fatal(err)
	}
	var ordinary map[string]string
	if err := json.Unmarshal(standalone, &ordinary); err != nil {
		t.Fatal(err)
	}
	if _, exists := ordinary["rest.client.max-retries"]; exists {
		t.Fatal("changed standalone REST retry policy")
	}
}

func TestExecutionFenceEnvironmentPreservesFilesCredentialsSeparately(t *testing.T) {
	original := []string{"AWS_ACCESS_KEY_ID=files-key", "AWS_SECRET_ACCESS_KEY=files-secret", "AWS_SESSION_TOKEN=files-token", "AWS_REGION=files-region", "AWS_PROFILE=files-profile", "OLAKE_EXECUTION_AWS_REGION=stale", "OTHER=keep"}
	before := append([]string(nil), original...)
	got := executionFenceEnvironment(original)
	if !reflect.DeepEqual(before, original) {
		t.Fatal("mutated parent environment")
	}
	for _, want := range []string{"OLAKE_EXECUTION_AWS_CAPTURED=true", "OLAKE_EXECUTION_AWS_ACCESS_KEY_ID=files-key", "OLAKE_EXECUTION_AWS_SECRET_ACCESS_KEY=files-secret", "OLAKE_EXECUTION_AWS_SESSION_TOKEN=files-token", "OLAKE_EXECUTION_AWS_REGION=files-region", "OLAKE_EXECUTION_AWS_PROFILE=files-profile", "OLAKE_EXECUTION_AWS_DEFAULT_REGION="} {
		found := false
		for _, entry := range got {
			if entry == want {
				found = true
			}
		}
		if !found {
			t.Fatalf("missing preserved runtime setting %q", strings.SplitN(want, "=", 2)[0])
		}
	}
	for _, entry := range got {
		if entry == "OLAKE_EXECUTION_AWS_REGION=stale" {
			t.Fatal("retained stale captured credentials")
		}
	}
}

func TestExecutionCapabilityRejectsOldOrMisconfiguredWriterBeforeMutation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "execution-fence.json")
	if err := os.WriteFile(path, []byte(`{"version":1,"execution_id":"runtime","state":"open"}`), 0600); err != nil {
		t.Fatal(err)
	}
	config := &Config{executionID: "runtime", executionFence: path, resyncLaunchID: "launch", resyncRecoverOnly: true}
	for _, test := range []struct {
		name, result string
		success      bool
		err          error
		allowed      bool
	}{
		{"supported", `{"version":1,"execution_id":"runtime","launch_id":"launch","recover_only":true,"fence_state":"open"}`, true, nil, true},
		{"old-unknown-payload", "", false, errors.New("unrecognized payload type"), false},
		{"old-generic-success", "Successfully processed", true, nil, false},
		{"rejected", `{}`, false, nil, false},
		{"ignored-startup-id", `{"version":1,"execution_id":"","launch_id":"launch","recover_only":true,"fence_state":"open"}`, true, nil, false},
		{"wrong-id", `{"version":1,"execution_id":"other","launch_id":"launch","recover_only":true,"fence_state":"open"}`, true, nil, false},
		{"wrong-launch", `{"version":1,"execution_id":"runtime","launch_id":"other","recover_only":true,"fence_state":"open"}`, true, nil, false},
		{"ignored-recovery", `{"version":1,"execution_id":"runtime","launch_id":"launch","recover_only":false,"fence_state":"open"}`, true, nil, false},
		{"closed", `{"version":1,"execution_id":"runtime","launch_id":"launch","recover_only":true,"fence_state":"closed"}`, true, nil, false},
		{"wrong-version", `{"version":2,"execution_id":"runtime","launch_id":"launch","recover_only":true,"fence_state":"open"}`, true, nil, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := &resyncControlClient{send: func(_ context.Context, request *proto.IcebergPayload) (*proto.RecordIngestResponse, error) {
				if request.GetType() != proto.IcebergPayload_EXECUTION_CAPABILITY {
					t.Fatal("startup dispatched table mutation before capability confirmation")
				}
				if request.GetMetadata().GetThreadId() != "" || request.GetMetadata().GetPayload() != "" {
					t.Fatal("capability must not open a session or echo request identity")
				}
				return &proto.RecordIngestResponse{Success: test.success, Result: test.result}, test.err
			}}
			err := (&serverInstance{client: client}).confirmExecutionCapability(context.Background(), config)
			if test.allowed && err != nil {
				t.Fatal(err)
			}
			if !test.allowed && err == nil {
				t.Fatal("accepted writer without configured fence/recovery capability")
			}
			if len(client.requests) != 1 {
				t.Fatal("capability handshake dispatched unexpected requests")
			}
		})
	}
}

func TestExecutionCapabilityMissingFenceStopsBeforeJVMRequest(t *testing.T) {
	client := &resyncControlClient{send: func(context.Context, *proto.IcebergPayload) (*proto.RecordIngestResponse, error) {
		t.Fatal("missing positive fence reached JVM")
		return nil, nil
	}}
	err := (&serverInstance{client: client}).confirmExecutionCapability(context.Background(), &Config{executionID: "runtime", executionFence: filepath.Join(t.TempDir(), "missing.json")})
	if err == nil {
		t.Fatal("missing runtime fence was accepted")
	}
}
