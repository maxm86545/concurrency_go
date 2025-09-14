package storage

import "sync"

type inMemoryEngine struct {
	m  map[string][]byte
	mu sync.Mutex
}

func newInMemoryEngine(initSize int) *inMemoryEngine {
	return &inMemoryEngine{
		m:  make(map[string][]byte, initSize),
		mu: sync.Mutex{},
	}
}

func (e *inMemoryEngine) Set(key []byte, value []byte) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.m[string(key)] = value
}

func (e *inMemoryEngine) Get(key []byte) ([]byte, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()

	value, ok := e.m[string(key)]

	return value, ok
}

func (e *inMemoryEngine) Del(key []byte) {
	e.mu.Lock()
	defer e.mu.Unlock()

	delete(e.m, string(key))
}
