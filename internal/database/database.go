package database

import (
	"context"
	"errors"
	"fmt"

	"go.uber.org/zap"

	"github.com/maxm86545/concurrency_go/internal/database/compute"
	"github.com/maxm86545/concurrency_go/internal/database/storage"
)

const loggerName = "database"

type iCompute interface {
	Parse(query []byte) (compute.Query, error)
}

type iStorage interface {
	Set(ctx context.Context, key []byte, value []byte) error
	Get(ctx context.Context, key []byte) ([]byte, error)
	Del(ctx context.Context, key []byte) error
}

type Database struct {
	compute iCompute
	storage iStorage
	logger  *zap.Logger
}

func NewDatabase(l *zap.Logger, c iCompute, s iStorage) *Database {
	return &Database{
		compute: c,
		storage: s,
		logger:  l.Named(loggerName),
	}
}

func (d *Database) Exec(ctx context.Context, rawQuery []byte) ExecResult {
	if err := ctx.Err(); err != nil {
		d.logger.Warn("context error", zap.Error(err))

		return ExecResult{Status: StatusErr, Err: err}
	}

	d.logger.Debug("parsing query", zap.ByteString("rawQuery", rawQuery))
	query, err := d.compute.Parse(rawQuery)
	if err != nil {
		d.logger.Warn("failed to parse query", zap.Error(err))

		return ExecResult{Status: StatusErr, Err: fmt.Errorf("parse query: %v", err)}
	}

	switch q := query.(type) {
	case *compute.SetQuery:
		d.logger.Debug("executing SET query", zap.ByteString("key", q.Key), zap.ByteString("value", q.Value))
		err := d.storage.Set(ctx, q.Key, q.Value)
		if err != nil {
			d.logger.Error("failed to execute SET", zap.ByteString("key", q.Key), zap.Error(err))

			return ExecResult{Status: StatusErr, Err: fmt.Errorf("set query: %v", err)}
		}

		d.logger.Info("SET query executed successfully", zap.ByteString("key", q.Key))

		return ExecResult{Status: StatusOkNoData}

	case *compute.GetQuery:
		d.logger.Debug("executing GET query", zap.ByteString("key", q.Key))
		result, err := d.storage.Get(ctx, q.Key)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				d.logger.Info("GET query: key not found", zap.ByteString("key", q.Key))

				return ExecResult{Status: StatusNotFound}
			}

			d.logger.Error("failed to execute GET", zap.ByteString("key", q.Key), zap.Error(err))

			return ExecResult{Status: StatusErr, Err: fmt.Errorf("get query: %v", err)}
		}

		d.logger.Info("GET query executed successfully", zap.ByteString("key", q.Key), zap.ByteString("value", result))

		return ExecResult{Status: StatusOK, Data: result}

	case *compute.DelQuery:
		d.logger.Debug("executing DEL query", zap.ByteString("key", q.Key))
		err := d.storage.Del(ctx, q.Key)
		if err != nil {
			d.logger.Error("failed to execute DEL", zap.ByteString("key", q.Key), zap.Error(err))

			return ExecResult{Status: StatusErr, Err: fmt.Errorf("del query: %v", err)}
		}

		d.logger.Info("DEL query executed successfully", zap.ByteString("key", q.Key))

		return ExecResult{Status: StatusOkNoData}
	}

	d.logger.Warn("unknown query type", zap.String("type", fmt.Sprintf("%T", query)))

	return ExecResult{Status: StatusUnsupported, Err: fmt.Errorf("unknown query type: %T", query)}
}
