// Package introspect contains a main introspecter interface which let you introspect a database for
//
//	the current state of it. It returns the schema.Database type with all information about the current database,
//
// or an error if connection/queries were unsuccessful.
package introspect

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	"smf/internal/schema"
)

type Introspecter interface {
	Introspect(ctx context.Context, db *sql.DB) (*schema.Database, error)
}

var (
	registry = make(map[schema.Dialect]func() Introspecter)
	mu       sync.RWMutex
)

func Register(dialect schema.Dialect, fn func() Introspecter) {
	mu.Lock()
	defer mu.Unlock()
	registry[dialect] = fn
}

func NewIntrospecter(dialect schema.Dialect) (Introspecter, error) {
	mu.RLock()
	fn, ok := registry[dialect]
	mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("unsupported dialect %v", dialect)
	}

	return fn(), nil
}
