package logger

import (
	"fmt"
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func MakeFileLogger(fileName string) (*zap.Logger, error) {
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
