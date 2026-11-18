package protocol

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func TestExecuteFinalizesSourceAuthenticationFailureBeforeDestination(t *testing.T) {
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

func TestExecutePreservesPublishedRecoveryCheckpoint(t *testing.T) {
	folder := t.TempDir()
	t.Setenv(constants.EnvS3ConfigFolder, "")
	previousFolder, previousPath, previousState := viper.Get(constants.ConfigFolder), viper.Get(constants.StatePath), state
	viper.Set(constants.ConfigFolder, folder)
	stateFile := filepath.Join(folder, "state.json")
	viper.Set(constants.StatePath, stateFile)
	t.Cleanup(func() {
		viper.Set(constants.ConfigFolder, previousFolder)
		viper.Set(constants.StatePath, previousPath)
		state = previousState
	})
	state = &types.State{RWMutex: &sync.RWMutex{}, Type: types.GlobalType, Global: &types.GlobalState{State: map[string]any{"lsn": "0/100"}, Streams: types.NewSet("public.a")}}
	published := `{"type":"GLOBAL","global":{"state":{"lsn":"0/200","large":9007199254740993},"streams":["public.a"]},"streams":[]}`
	if err := os.WriteFile(stateFile, []byte(published), 0600); err != nil {
		t.Fatal(err)
	}
	ctx := utils.WithExecution(context.Background(), &utils.ExecutionOptions{ID: "recovery", LaunchID: "original", Fence: "fence"})
	utils.BeginOrdinaryExecution(ctx, "sync")
	ctx = types.WithOrdinaryRecovery(ctx, &types.OrdinaryRecoveryOptions{})
	cmd := &cobra.Command{Use: "sync", SilenceErrors: true, SilenceUsage: true,
		RunE: func(*cobra.Command, []string) error {
			utils.BeginExecutionParticipant(ctx)(true)
			utils.SourceDrained(ctx, nil)
			return nil
		},
	}
	cmd.SetContext(ctx)
	cmd.SetArgs([]string{})
	if err := Execute(cmd); err != nil {
		t.Fatal(err)
	}
	saved, err := os.ReadFile(stateFile)
	if err != nil || string(saved) != published {
		t.Fatalf("finalization replaced the proof-bound checkpoint: %s %v", saved, err)
	}
	digest, err := utils.CanonicalStateSHA256(saved)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(folder, "execution-result.json"))
	if err != nil {
		t.Fatal(err)
	}
	var receipt utils.ExecutionResult
	if err := json.Unmarshal(data, &receipt); err != nil {
		t.Fatal(err)
	}
	if receipt.Outcome != "settled" || receipt.StateSHA256 != digest {
		t.Fatalf("receipt does not bind the published checkpoint: %+v", receipt)
	}
}
