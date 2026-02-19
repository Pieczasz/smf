package db2

import (
	"context"
	"database/sql"

	"smf/internal/core"
	"smf/internal/introspect"
)

func init() {
	introspect.Register(core.DialectDB2, New)
}

type introspecter struct{}

func New() introspect.Introspecter {
	return &introspecter{}
}

func (i *introspecter) Introspect(_ context.Context, _ *sql.DB) (*core.Database, error) {
	return nil, nil
}
