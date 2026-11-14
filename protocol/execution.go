package protocol

import (
	"context"
	"errors"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	s3util "github.com/datazip-inc/olake/utils/s3"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// Execute finalizes controlled ordinary commands even when a Cobra pre/run/post
// hook fails. It never derives retirement from the command's exit status.
func Execute(root *cobra.Command) error {
	cmd, runErr := root.ExecuteC()
	if cmd == nil {
		return runErr
	}
	if cmd.Name() == "clear-destination" {
		utils.SourceDrained(cmd.Context(), nil)
	}
	result, controlled := utils.OrdinaryExecutionResult(cmd.Context())
	if !controlled {
		return runErr
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	var proofErr error
	if result.Outcome == "settled" {
		stateFile := viper.GetString(constants.StatePath)
		// Read has joined before SourceDrained; persist its complete acknowledged
		// checkpoint even when RunE returned a local record-processing error.
		if cmd.Name() == "sync" && state != nil {
			proofErr = logger.FileLoggerWithPath(state, stateFile)
		}
		var saved []byte
		if proofErr == nil {
			saved, proofErr = os.ReadFile(stateFile)
		}
		if proofErr == nil {
			result.StateSHA256, proofErr = utils.CanonicalStateSHA256(saved)
		}
		if proofErr == nil && s3util.JobBucket != "" {
			proofErr = s3util.UploadFileToS3(ctx, stateFile, s3util.JobBucket, path.Join(s3util.JobPrefix, "state.json"))
		}
		if proofErr != nil {
			result.Outcome = "unknown"
			result.StateSHA256 = ""
		}
	}
	localPath, uri, resolveErr := utils.ResolveArtifactOutput(filepath.Join(viper.GetString(constants.ConfigFolder), "execution-result.json"), "execution-result.json", configPath)
	if resolveErr != nil {
		return errors.Join(runErr, proofErr, resolveErr)
	}
	if result.Outcome == "unknown" {
		runErr = errors.Join(runErr, errors.New("execution retirement remains unknown; retain ownership"))
	}
	return errors.Join(runErr, proofErr, utils.PublishExecutionResult(ctx, localPath, uri, result))
}
