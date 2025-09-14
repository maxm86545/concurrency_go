package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/multierr"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/sync/errgroup"

	"github.com/maxm86545/concurrency_go/internal/cli"
	"github.com/maxm86545/concurrency_go/internal/database"
	"github.com/maxm86545/concurrency_go/internal/database/compute"
	"github.com/maxm86545/concurrency_go/internal/database/storage"
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

	logger, err := makeLogger("app.log")
	if err != nil {
		return fmt.Errorf("create logger: %w", err)
	}
	defer multierr.AppendFunc(&errReturned, logger.Sync)

	db := database.NewDatabase(
		logger,
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

func makeLogger(fileName string) (*zap.Logger, error) {
	f, err := os.OpenFile(fileName, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open log file: %w", err)
	}

	cfg := zap.NewProductionConfig()
	cfg.OutputPaths = []string{}
	cfg.DisableStacktrace = true
	cfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	return cfg.Build(zap.WrapCore(func(core zapcore.Core) zapcore.Core {
		fileCore := zapcore.NewCore(
			zapcore.NewJSONEncoder(cfg.EncoderConfig),
			zapcore.AddSync(f),
			cfg.Level,
		)

		return zapcore.NewTee(core, fileCore)
	}))
}
