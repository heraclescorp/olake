package iceberg

import (
	"context"
	"encoding/json"

	"github.com/datazip-inc/olake/destination/iceberg/proto"
	"github.com/datazip-inc/olake/utils"
)

func validExecutionRetirement(data string, config *Config) bool {
	var result utils.ExecutionResult
	if json.Unmarshal([]byte(data), &result) != nil {
		return false
	}
	return result.Version == 1 && result.ExecutionID == config.executionID && result.LaunchID == config.executionID && result.Command == config.executionCommand && (result.Outcome == "settled" || result.Outcome == "not_started")
}

// This direct terminal RPC deliberately permits CLOSED fences. The Java side
// validates the actual startup identity and drains its own admitted requests.
func (s *serverInstance) confirmExecutionRetirement(ctx context.Context, config *Config) bool {
	if config == nil || config.executionCommand == "" {
		return false
	}
	response, err := s.client.SendRecords(ctx, &proto.IcebergPayload{
		Type:     proto.IcebergPayload_EXECUTION_RETIRE,
		Metadata: &proto.IcebergPayload_Metadata{},
	})
	return err == nil && response != nil && response.GetSuccess() && validExecutionRetirement(response.GetResult(), config)
}
