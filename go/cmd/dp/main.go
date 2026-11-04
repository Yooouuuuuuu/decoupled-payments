// Command dp runs one role of the decoupled payment system: init, generator,
// aggregator, validator or report. Run dp --help for usage.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/Yooouuuuuuu/decoupled-payments/go/internal/cli"
)

func main() {
	// SIGINT or SIGTERM cancels ctx. The generator, aggregators and
	// validators then finish what they are doing, close their clients and
	// exit 0, also while they are still starting; init and report stop
	// without a result and exit 1.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := cli.Run(ctx, os.Args[1:], cli.Env{
		Stdout:    os.Stdout,
		Stderr:    os.Stderr,
		LookupEnv: os.LookupEnv,
		ReadFile:  os.ReadFile,
	})
	stop()
	os.Exit(code)
}
