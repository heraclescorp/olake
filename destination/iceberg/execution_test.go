package iceberg

import "testing"

func TestExecutionRetirementRequiresActualMatchingTerminal(t *testing.T) {
	config := &Config{executionID: "runtime", executionCommand: "sync"}
	for _, test := range []struct {
		data  string
		valid bool
	}{
		{`{"version":1,"execution_id":"runtime","launch_id":"runtime","command":"sync","outcome":"settled"}`, true},
		{`{"version":1,"execution_id":"runtime","launch_id":"runtime","command":"sync","outcome":"not_started"}`, true},
		{`{"version":1,"execution_id":"other","launch_id":"runtime","command":"sync","outcome":"settled"}`, false},
		{`{"version":1,"execution_id":"runtime","launch_id":"old","command":"sync","outcome":"settled"}`, false},
		{`{"version":1,"execution_id":"runtime","launch_id":"runtime","command":"clear-destination","outcome":"settled"}`, false},
		{`{"version":1,"execution_id":"runtime","launch_id":"runtime","command":"sync","outcome":"unknown"}`, false},
		{`{"version":2,"execution_id":"runtime","launch_id":"runtime","command":"sync","outcome":"settled"}`, false},
		{`Successfully processed`, false},
	} {
		if got := validExecutionRetirement(test.data, config); got != test.valid {
			t.Fatalf("accepted=%v for %s", got, test.data)
		}
	}
}
