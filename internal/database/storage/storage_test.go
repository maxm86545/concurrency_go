package storage_test

import (
	"bytes"
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/maxm86545/concurrency_go/internal/database/storage"
)

func TestStorageOperations(t *testing.T) {
	type testCase struct {
		name    string
		setup   func(s *storage.Storage)
		action  func(s *storage.Storage) ([]byte, error)
		want    []byte
		wantErr error
	}

	tests := []testCase{
		{
			name: "Set and Get success",
			setup: func(s *storage.Storage) {
				_ = s.Set(context.Background(), []byte("foo"), []byte("bar"))
			},
			action: func(s *storage.Storage) ([]byte, error) {
				return s.Get(context.Background(), []byte("foo"))
			},
			want:    []byte("bar"),
			wantErr: nil,
		},
		{
			name:  "Get missing key",
			setup: func(_ *storage.Storage) {},
			action: func(s *storage.Storage) ([]byte, error) {
				return s.Get(context.Background(), []byte("missing"))
			},
			want:    nil,
			wantErr: storage.ErrNotFound,
		},
		{
			name:  "Del missing key",
			setup: func(_ *storage.Storage) {},
			action: func(s *storage.Storage) ([]byte, error) {
				err := s.Del(context.Background(), []byte("ghost"))
				return nil, err
			},
			want:    nil,
			wantErr: nil,
		},
		{
			name: "Del removes key",
			setup: func(s *storage.Storage) {
				_ = s.Set(context.Background(), []byte("key"), []byte("value"))
				_ = s.Del(context.Background(), []byte("key"))
			},
			action: func(s *storage.Storage) ([]byte, error) {
				return s.Get(context.Background(), []byte("key"))
			},
			want:    nil,
			wantErr: storage.ErrNotFound,
		},
		{
			name: "Overwrite existing key",
			setup: func(s *storage.Storage) {
				_ = s.Set(context.Background(), []byte("dup"), []byte("first"))
				_ = s.Set(context.Background(), []byte("dup"), []byte("second"))
			},
			action: func(s *storage.Storage) ([]byte, error) {
				return s.Get(context.Background(), []byte("dup"))
			},
			want:    []byte("second"),
			wantErr: nil,
		},
		{
			name: "Set and Get empty value",
			setup: func(s *storage.Storage) {
				_ = s.Set(context.Background(), []byte("empty"), []byte{})
			},
			action: func(s *storage.Storage) ([]byte, error) {
				return s.Get(context.Background(), []byte("empty"))
			},
			want:    []byte{},
			wantErr: nil,
		},
		{
			name: "Set and Get empty key",
			setup: func(s *storage.Storage) {
				_ = s.Set(context.Background(), []byte{}, []byte("val"))
			},
			action: func(s *storage.Storage) ([]byte, error) {
				return s.Get(context.Background(), []byte{})
			},
			want:    []byte("val"),
			wantErr: nil,
		},
		{
			name:  "Set with canceled context",
			setup: func(_ *storage.Storage) {},
			action: func(s *storage.Storage) ([]byte, error) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				err := s.Set(ctx, []byte("x"), []byte("y"))
				return nil, err
			},
			want:    nil,
			wantErr: context.Canceled,
		},
		{
			name: "Get with deadline exceeded",
			setup: func(s *storage.Storage) {
				_ = s.Set(context.Background(), []byte("a"), []byte("b"))
			},
			action: func(s *storage.Storage) ([]byte, error) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
				defer cancel()
				time.Sleep(time.Microsecond)
				return s.Get(ctx, []byte("a"))
			},
			want:    nil,
			wantErr: context.DeadlineExceeded,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := storage.NewStorage()
			tc.setup(s)
			result, err := tc.action(s)

			require.ErrorIs(t, err, tc.wantErr)
			assert.True(t, bytes.Equal(result, tc.want), "expected %q, got %q", tc.want, result)
		})
	}
}

func TestConcurrentSetGet(t *testing.T) {
	const workers = 100

	s := storage.NewStorage()
	ctx := context.Background()

	var wg sync.WaitGroup

	runConcurrent(workers, &wg, func(i int) {
		key, val := generateKV(i)
		assert.NoError(t, s.Set(ctx, key, val))
	})

	runConcurrent(workers, &wg, func(i int) {
		key, expected := generateKV(i)
		val, err := s.Get(ctx, key)
		require.NoError(t, err)
		assert.Equal(t, expected, val)
	})
}

func TestCustomEngine(t *testing.T) {
	ctx := context.Background()

	type testCase struct {
		name     string
		setup    func(*mockEngine)
		action   func(*storage.Storage) ([]byte, error)
		expected []byte
		wantErr  error
	}

	tests := []testCase{
		{
			name: "Set delegates to engine",
			setup: func(m *mockEngine) {
				m.setFunc = func(key, value []byte) {
					assert.Equal(t, []byte("foo"), key)
					assert.Equal(t, []byte("bar"), value)
				}
			},
			action: func(s *storage.Storage) ([]byte, error) {
				err := s.Set(ctx, []byte("foo"), []byte("bar"))
				return nil, err
			},
			expected: nil,
			wantErr:  nil,
		},
		{
			name: "Get returns value from engine",
			setup: func(m *mockEngine) {
				m.getFunc = func(key []byte) ([]byte, bool) {
					if bytes.Equal(key, []byte("foo")) {
						return []byte("bar"), true
					}
					return nil, false
				}
			},
			action: func(s *storage.Storage) ([]byte, error) {
				return s.Get(ctx, []byte("foo"))
			},
			expected: []byte("bar"),
			wantErr:  nil,
		},
		{
			name: "Get returns ErrNotFound",
			setup: func(m *mockEngine) {
				m.getFunc = func(_ []byte) ([]byte, bool) {
					return nil, false
				}
			},
			action: func(s *storage.Storage) ([]byte, error) {
				return s.Get(ctx, []byte("missing"))
			},
			expected: nil,
			wantErr:  storage.ErrNotFound,
		},
		{
			name: "Del delegates to engine",
			setup: func(m *mockEngine) {
				m.delFunc = func(key []byte) {
					assert.Equal(t, []byte("foo"), key)
				}
			},
			action: func(s *storage.Storage) ([]byte, error) {
				err := s.Del(ctx, []byte("foo"))
				return nil, err
			},
			expected: nil,
			wantErr:  nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mock := &mockEngine{}
			tc.setup(mock)
			s := storage.NewStorageWithEngine(mock)

			result, err := tc.action(s)

			require.ErrorIs(t, err, tc.wantErr)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func FuzzStorage(f *testing.F) {
	ctx := context.Background()
	s := storage.NewStorage()

	f.Add([]byte("key"), []byte("value"))
	f.Add([]byte{}, []byte{})
	f.Add([]byte("null"), []byte(nil))
	f.Add([]byte(nil), []byte(nil))

	f.Fuzz(func(t *testing.T, key, value []byte) {
		require.NoError(t, s.Set(ctx, key, value), "Set should not fail")

		result, err := s.Get(ctx, key)
		require.NoError(t, err, "Get should not fail")
		require.Equal(t, value, result, "Get returned unexpected value")

		require.NoError(t, s.Del(ctx, key), "Del should not fail")

		result, err = s.Get(ctx, key)
		require.Nil(t, result, "Result should be nil after deletion")
		require.ErrorIs(t, err, storage.ErrNotFound, "Expected ErrNotFound after Del")
	})
}

type mockEngine struct {
	setFunc func(key, value []byte)
	getFunc func(key []byte) ([]byte, bool)
	delFunc func(key []byte)
}

func (m *mockEngine) Set(key, value []byte) {
	if m.setFunc == nil {
		panic("setFunc is nil")
	}
	m.setFunc(key, value)
}

func (m *mockEngine) Get(key []byte) ([]byte, bool) {
	if m.getFunc == nil {
		panic("getFunc is nil")
	}
	return m.getFunc(key)
}

func (m *mockEngine) Del(key []byte) {
	if m.delFunc == nil {
		panic("delFunc is nil")
	}
	m.delFunc(key)
}

func runConcurrent(n int, wg *sync.WaitGroup, fn func(i int)) {
	wg.Add(n)
	for i := range n {
		go func(i int) {
			defer wg.Done()
			fn(i)
		}(i)
	}
	wg.Wait()
}

func generateKV(i int) (key []byte, val []byte) {
	suffix := strconv.Itoa(i)
	key = []byte("key" + suffix)
	val = []byte("val" + suffix)

	return key, val
}
