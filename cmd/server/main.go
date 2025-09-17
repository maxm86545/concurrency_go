package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/multierr"
	"golang.org/x/sync/errgroup"

	"github.com/maxm86545/concurrency_go/internal/cli"
	"github.com/maxm86545/concurrency_go/internal/database"
	"github.com/maxm86545/concurrency_go/internal/database/compute"
	"github.com/maxm86545/concurrency_go/internal/database/storage"
	"github.com/maxm86545/concurrency_go/internal/logger"
)

const maxCommandLen = 128

func main() {
	if err := run(); err != nil {
		panic(err)
	}
}

func run() (errReturned error) {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log, err := logger.MakeFileLogger("app.log")
	if err != nil {
		return fmt.Errorf("create logger: %w", err)
	}
	defer multierr.AppendFunc(&errReturned, log.Sync)

	db := database.NewDatabase(
		log,
		compute.NewCompute(maxCommandLen),
		storage.NewStorage(),
	)

	cliApp, err := cli.NewCliApp(
		os.Stdin,
		os.Stdout,
		os.Stderr,
		db,
	)
	if err != nil {
		return fmt.Errorf("create cli app: %w", err)
	}

	eg, egCtx := errgroup.WithContext(ctx)

	err = cliApp.WriteHelp()
	if err != nil {
		return fmt.Errorf("write help: %w", err)
	}

	eg.Go(func() error {
		return cliApp.Run(egCtx)
	})

	return eg.Wait()
}
