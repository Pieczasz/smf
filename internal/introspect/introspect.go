// Package introspect contains a main introspecter interface which let you introspect a database for
// current state of it. It returns core.Database type with all information about current database,
// or an error if connection/queries were unsuccessful.
package introspect

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	"smf/internal/core"
)

type Introspecter interface {
	Introspect(ctx context.Context, db *sql.DB) (*core.Database, error)
}

var (
	registry = make(map[core.Dialect]func() Introspecter)
	mu       sync.RWMutex
)

func Register(dialect core.Dialect, fn func() Introspecter) {
	mu.Lock()
	defer mu.Unlock()
	registry[dialect] = fn
}

func NewIntrospecter(dialect core.Dialect) (Introspecter, error) {
	mu.RLock()
	fn, ok := registry[dialect]
	mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("unsupported dialect %v", dialect)
	}

	return fn(), nil
}
