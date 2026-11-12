package protocol

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/utils"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func TestExecuteFinalizesSourceAuthenticationFailureBeforeJVM(t *testing.T) {
	folder := t.TempDir()
	previous := viper.Get(constants.ConfigFolder)
	viper.Set(constants.ConfigFolder, folder)
	defer viper.Set(constants.ConfigFolder, previous)
	ctx := utils.WithExecution(context.Background(), &utils.ExecutionOptions{ID: "runtime", Fence: "fence"})
	utils.BeginOrdinaryExecution(ctx, "sync")
	badPassword := errors.New("source password authentication failed")
	cmd := &cobra.Command{Use: "sync", SilenceErrors: true, SilenceUsage: true,
		PreRunE: func(*cobra.Command, []string) error { return badPassword },
		RunE:    func(*cobra.Command, []string) error { t.Fatal("run after failed authentication"); return nil },
	}
	cmd.SetContext(ctx)
	cmd.SetArgs([]string{})
	if err := Execute(cmd); !errors.Is(err, badPassword) {
		t.Fatalf("lost source error: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(folder, "execution-result.json"))
	if err != nil {
		t.Fatal(err)
	}
	var result utils.ExecutionResult
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatal(err)
	}
	if result.Outcome != "not_started" || result.ExecutionID != "runtime" || result.LaunchID != "runtime" || result.Command != "sync" || result.StateSHA256 != "" {
		t.Fatalf("bad proof: %+v", result)
	}
}

func TestExecuteReportsUnknownRetirementAfterSuccessfulRun(t *testing.T) {
	folder := t.TempDir()
	previous := viper.Get(constants.ConfigFolder)
	viper.Set(constants.ConfigFolder, folder)
	defer viper.Set(constants.ConfigFolder, previous)
	ctx := utils.WithExecution(context.Background(), &utils.ExecutionOptions{ID: "runtime", Fence: "fence"})
	utils.BeginOrdinaryExecution(ctx, "sync")
	cmd := &cobra.Command{Use: "sync", SilenceErrors: true, SilenceUsage: true,
		RunE: func(*cobra.Command, []string) error {
			utils.BeginExecutionParticipant(ctx)(false)
			utils.SourceDrained(ctx, nil)
			return nil
		},
	}
	cmd.SetContext(ctx)
	cmd.SetArgs([]string{})
	if err := Execute(cmd); err == nil {
		t.Fatal("successful command hid unknown execution retirement")
	}
	data, err := os.ReadFile(filepath.Join(folder, "execution-result.json"))
	if err != nil {
		t.Fatal(err)
	}
	var result utils.ExecutionResult
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatal(err)
	}
	if result.Outcome != "unknown" || result.StateSHA256 != "" {
		t.Fatalf("unknown retirement published invalid evidence: %+v", result)
	}
}
