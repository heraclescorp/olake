package olake

import (
	"context"
	"os"
	"time"

	_ "github.com/datazip-inc/olake/destination/iceberg" // registering iceberg destination
	_ "github.com/datazip-inc/olake/destination/parquet" // registering parquet destination
	"github.com/datazip-inc/olake/drivers/abstract"
	protocol "github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/datazip-inc/olake/utils/observability"
	"github.com/datazip-inc/olake/utils/safego"
	"github.com/spf13/cobra"
)

func RegisterDriver(driver abstract.DriverInterface) {
	defer safego.Recovery(true)

	// Keep diagnostics inside a returning scope: os.Exit and logger.Fatal do
	// not run defers. This also flushes before the outer panic recovery exits.
	err := func() error {
		root := protocol.CreateRootCommand(true, driver)
		shutdown := func(context.Context) error { return nil }
		before := root.PersistentPreRunE
		root.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
			if before != nil {
				if err := before(cmd, args); err != nil {
					return err
				}
			}
			var startErr error
			shutdown, startErr = observability.Start(cmd.Context())
			if startErr != nil {
				logger.Warnf("Go diagnostics initialization skipped: %v", startErr)
			}
			return nil
		}
		defer func() { root.PersistentPreRunE = before }()
		defer func() {
			// A signal may have canceled the ingestion context already.
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := shutdown(ctx); err != nil {
				logger.Warnf("Go diagnostics shutdown failed: %v", err)
			}
		}()
		return root.Execute()
	}()
	if err != nil {
		protocol.ReportFailure(err)
		logger.Fatal(err)
	}

	os.Exit(0)
}
