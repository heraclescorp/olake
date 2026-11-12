package iceberg

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"testing"

	"github.com/datazip-inc/olake/utils/logger"
	"github.com/stretchr/testify/require"
)

func TestJVMShutdownSharesStartupWaiter(t *testing.T) {
	for _, mode := range []string{"terminate", "exit-zero", "exit-seven"} {
		t.Run(mode, func(t *testing.T) {
			input, writer, err := os.Pipe()
			require.NoError(t, err)
			defer input.Close()
			defer writer.Close()
			cmd := exec.Command(os.Args[0], "-test.run=^TestJVMProcessHelper$")
			cmd.Env = append(os.Environ(), "OLAKE_TEST_JVM_PROCESS="+mode)
			cmd.Stdin = input
			exited, err := logger.SetupAndStartProcess("JVM shutdown test", cmd)
			require.NoError(t, err)
			instance := &serverInstance{cmd: cmd, processExited: exited}
			if mode != "terminate" {
				require.NoError(t, writer.Close())
			}
			err = instance.shutdownResync()
			if mode == "exit-seven" {
				require.ErrorContains(t, err, "exit status 7")
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, err, instance.shutdownResync())
			require.NotNil(t, cmd.ProcessState)
		})
	}
}

func TestJVMProcessHelper(t *testing.T) {
	mode := os.Getenv("OLAKE_TEST_JVM_PROCESS")
	if mode == "" {
		return
	}
	if mode == "terminate" {
		termination := make(chan os.Signal, 1)
		signal.Notify(termination, syscall.SIGTERM)
		fmt.Println("Server started on port 50051")
		<-termination
		os.Exit(0)
	}
	signal.Ignore(syscall.SIGTERM)
	fmt.Println("Server started on port 50051")
	_, _ = io.Copy(io.Discard, os.Stdin)
	if mode == "exit-seven" {
		os.Exit(7)
	}
	os.Exit(0)
}
