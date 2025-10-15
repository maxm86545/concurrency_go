package storage

import (
	"context"
	"errors"
)

const initSize = 1024

var ErrNotFound = errors.New("storage: not found")

type iEngine interface {
	Set(key []byte, value []byte)
	Get(key []byte) ([]byte, bool)
	Del(key []byte)
}

type Storage struct {
	engine iEngine
}

func NewStorage() *Storage {
	return &Storage{
		engine: newInMemoryEngine(initSize),
	}
}

func NewStorageWithEngine(engine iEngine) *Storage {
	return &Storage{
		engine: engine,
	}
}

func (s *Storage) Set(ctx context.Context, key []byte, value []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.engine.Set(key, value)

	return nil
}

func (s *Storage) Get(ctx context.Context, key []byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	value, ok := s.engine.Get(key)
	if !ok {
		return nil, ErrNotFound
	}

	return value, nil
}

func (s *Storage) Del(ctx context.Context, key []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.engine.Del(key)

	return nil
}
