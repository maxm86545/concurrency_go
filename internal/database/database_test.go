package database_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
	"go.uber.org/zap/zaptest/observer"

	"github.com/maxm86545/concurrency_go/internal/database"
	"github.com/maxm86545/concurrency_go/internal/database/compute"
	"github.com/maxm86545/concurrency_go/internal/database/storage"
)

func TestDatabase_Exec(t *testing.T) {
	tests := []struct {
		name         string
		rawQuery     []byte
		compute      *mockCompute
		storage      *mockStorage
		wantStatus   database.ExecStatus
		wantData     []byte
		expectedLogs []expectedLog
	}{
		{
			name:     "set query success",
			rawQuery: []byte("set"),
			compute: &mockCompute{
				parseFn: func(_ []byte) (compute.Query, error) {
					return &compute.SetQuery{Key: []byte("k"), Value: []byte("v")}, nil
				},
			},
			storage: &mockStorage{
				setFunc: func(_ context.Context, key, val []byte) error {
					assert.Equal(t, []byte("k"), key)
					assert.Equal(t, []byte("v"), val)
					return nil
				},
			},
			wantStatus: database.StatusOkNoData,
			expectedLogs: []expectedLog{
				{Message: "parsing query", Level: zapcore.DebugLevel},
				{Message: "executing SET query", Level: zapcore.DebugLevel},
				{Message: "SET query executed successfully", Level: zapcore.InfoLevel},
			},
		},
		{
			name:     "set query with empty value",
			rawQuery: []byte("set"),
			compute: &mockCompute{
				parseFn: func(_ []byte) (compute.Query, error) {
					return &compute.SetQuery{Key: []byte("empty"), Value: []byte{}}, nil
				},
			},
			storage: &mockStorage{
				setFunc: func(_ context.Context, key, val []byte) error {
					assert.Equal(t, []byte("empty"), key)
					assert.Equal(t, []byte{}, val)
					return nil
				},
			},
			wantStatus: database.StatusOkNoData,
			expectedLogs: []expectedLog{
				{Message: "parsing query", Level: zapcore.DebugLevel},
				{Message: "executing SET query", Level: zapcore.DebugLevel},
				{Message: "SET query executed successfully", Level: zapcore.InfoLevel},
			},
		},
		{
			name:     "get query found",
			rawQuery: []byte("get"),
			compute: &mockCompute{
				parseFn: func(_ []byte) (compute.Query, error) {
					return &compute.GetQuery{Key: []byte("k")}, nil
				},
			},
			storage: &mockStorage{
				getFunc: func(_ context.Context, key []byte) ([]byte, error) {
					assert.Equal(t, []byte("k"), key)
					return []byte("value"), nil
				},
			},
			wantStatus: database.StatusOK,
			wantData:   []byte("value"),
			expectedLogs: []expectedLog{
				{Message: "parsing query", Level: zapcore.DebugLevel},
				{Message: "executing GET query", Level: zapcore.DebugLevel},
				{Message: "GET query executed successfully", Level: zapcore.InfoLevel},
			},
		},
		{
			name:     "get query not found",
			rawQuery: []byte("get"),
			compute: &mockCompute{
				parseFn: func(_ []byte) (compute.Query, error) {
					return &compute.GetQuery{Key: []byte("missing")}, nil
				},
			},
			storage: &mockStorage{
				getFunc: func(_ context.Context, key []byte) ([]byte, error) {
					assert.Equal(t, []byte("missing"), key)
					return nil, storage.ErrNotFound
				},
			},
			wantStatus: database.StatusNotFound,
			expectedLogs: []expectedLog{
				{Message: "parsing query", Level: zapcore.DebugLevel},
				{Message: "executing GET query", Level: zapcore.DebugLevel},
				{Message: "GET query: key not found", Level: zapcore.InfoLevel},
			},
		},
		{
			name:     "del query success",
			rawQuery: []byte("del"),
			compute: &mockCompute{
				parseFn: func(_ []byte) (compute.Query, error) {
					return &compute.DelQuery{Key: []byte("k")}, nil
				},
			},
			storage: &mockStorage{
				delFunc: func(_ context.Context, key []byte) error {
					assert.Equal(t, []byte("k"), key)
					return nil
				},
			},
			wantStatus: database.StatusOkNoData,
			expectedLogs: []expectedLog{
				{Message: "parsing query", Level: zapcore.DebugLevel},
				{Message: "executing DEL query", Level: zapcore.DebugLevel},
				{Message: "DEL query executed successfully", Level: zapcore.InfoLevel},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, observed := newObservedLogger()

			db := database.NewDatabase(
				logger,
				tt.compute,
				tt.storage,
			)
			result := db.Exec(context.Background(), tt.rawQuery)

			require.NoError(t, result.Err)
			assert.Equal(t, tt.wantStatus, result.Status)
			assert.Equal(t, tt.wantData, result.Data)

			logs := observed.All()
			require.Len(t, logs, len(tt.expectedLogs))
			for i, expected := range tt.expectedLogs {
				assert.Equal(t, expected.Message, logs[i].Message, "log message mismatch at index %d", i)
				assert.Equal(t, expected.Level, logs[i].Level, "log level mismatch at index %d", i)
			}
		})
	}
}

func TestDatabase_ExecInvalid(t *testing.T) {
	tests := []struct {
		name         string
		rawQuery     []byte
		compute      *mockCompute
		storage      *mockStorage
		wantStatus   database.ExecStatus
		wantErr      string
		expectedLogs []expectedLog
	}{
		{
			name:     "parse error",
			rawQuery: []byte("bad"),
			compute: &mockCompute{
				parseFn: func(_ []byte) (compute.Query, error) {
					return nil, errors.New("parse fail")
				},
			},
			storage:    &mockStorage{},
			wantStatus: database.StatusErr,
			wantErr:    "parse query: parse fail",
			expectedLogs: []expectedLog{
				{Message: "parsing query", Level: zapcore.DebugLevel},
				{Message: "failed to parse query", Level: zapcore.WarnLevel},
			},
		},
		{
			name:     "storage error on set",
			rawQuery: []byte("set"),
			compute: &mockCompute{
				parseFn: func(_ []byte) (compute.Query, error) {
					return &compute.SetQuery{Key: []byte("fail"), Value: []byte("v")}, nil
				},
			},
			storage: &mockStorage{
				setFunc: func(_ context.Context, _, _ []byte) error {
					return errors.New("set failed")
				},
			},
			wantStatus: database.StatusErr,
			wantErr:    "set query: set failed",
			expectedLogs: []expectedLog{
				{Message: "parsing query", Level: zapcore.DebugLevel},
				{Message: "executing SET query", Level: zapcore.DebugLevel},
				{Message: "failed to execute SET", Level: zapcore.ErrorLevel},
			},
		},
		{
			name:     "storage error on get",
			rawQuery: []byte("get"),
			compute: &mockCompute{
				parseFn: func(_ []byte) (compute.Query, error) {
					return &compute.GetQuery{Key: []byte("fail")}, nil
				},
			},
			storage: &mockStorage{
				getFunc: func(_ context.Context, _ []byte) ([]byte, error) {
					return nil, errors.New("get failed")
				},
			},
			wantStatus: database.StatusErr,
			wantErr:    "get query: get failed",
			expectedLogs: []expectedLog{
				{Message: "parsing query", Level: zapcore.DebugLevel},
				{Message: "executing GET query", Level: zapcore.DebugLevel},
				{Message: "failed to execute GET", Level: zapcore.ErrorLevel},
			},
		},
		{
			name:     "storage error on del",
			rawQuery: []byte("del"),
			compute: &mockCompute{
				parseFn: func(_ []byte) (compute.Query, error) {
					return &compute.DelQuery{Key: []byte("fail")}, nil
				},
			},
			storage: &mockStorage{
				delFunc: func(_ context.Context, _ []byte) error {
					return errors.New("del failed")
				},
			},
			wantStatus: database.StatusErr,
			wantErr:    "del query: del failed",
			expectedLogs: []expectedLog{
				{Message: "parsing query", Level: zapcore.DebugLevel},
				{Message: "executing DEL query", Level: zapcore.DebugLevel},
				{Message: "failed to execute DEL", Level: zapcore.ErrorLevel},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, observed := newObservedLogger()

			db := database.NewDatabase(
				logger,
				tt.compute,
				tt.storage,
			)
			result := db.Exec(context.Background(), tt.rawQuery)

			require.Error(t, result.Err)
			require.Errorf(t, result.Err, tt.wantErr)
			assert.Equal(t, tt.wantStatus, result.Status)

			logs := observed.All()
			require.Len(t, logs, len(tt.expectedLogs))
			for i, expected := range tt.expectedLogs {
				assert.Equal(t, expected.Message, logs[i].Message, "log message mismatch at index %d", i)
				assert.Equal(t, expected.Level, logs[i].Level, "log level mismatch at index %d", i)
			}
		})
	}
}

func TestDatabase_ExecCanceledContext(t *testing.T) {
	db := database.NewDatabase(
		zaptest.NewLogger(t),
		&mockCompute{
			parseFn: func(_ []byte) (compute.Query, error) {
				return &compute.GetQuery{Key: []byte("k")}, nil
			},
		},
		&mockStorage{},
	)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	result := db.Exec(ctx, []byte("any"))

	require.Error(t, result.Err)
	assert.Equal(t, database.StatusErr, result.Status)
	assert.ErrorIs(t, result.Err, context.Canceled)
}

type mockCompute struct {
	parseFn func([]byte) (compute.Query, error)
}

func (m *mockCompute) Parse(q []byte) (compute.Query, error) {
	return m.parseFn(q)
}

type mockStorage struct {
	setFunc func(context.Context, []byte, []byte) error
	getFunc func(context.Context, []byte) ([]byte, error)
	delFunc func(context.Context, []byte) error
}

func (m *mockStorage) Set(ctx context.Context, key, val []byte) error {
	if m.setFunc == nil {
		panic("setFunc is nil")
	}
	return m.setFunc(ctx, key, val)
}

func (m *mockStorage) Get(ctx context.Context, key []byte) ([]byte, error) {
	if m.getFunc == nil {
		panic("getFunc is nil")
	}
	return m.getFunc(ctx, key)
}

func (m *mockStorage) Del(ctx context.Context, key []byte) error {
	if m.delFunc == nil {
		panic("delFunc is nil")
	}
	return m.delFunc(ctx, key)
}

func newObservedLogger() (*zap.Logger, *observer.ObservedLogs) {
	core, logs := observer.New(zapcore.DebugLevel)
	logger := zap.New(core)

	return logger, logs
}

type expectedLog struct {
	Message string
	Level   zapcore.Level
}
