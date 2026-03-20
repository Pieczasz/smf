package db2

import (
	"context"
	"database/sql"

	"smf/internal/introspect"
	"smf/internal/schema"
)

func init() {
	introspect.Register(schema.DialectDB2, New)
}

type introspecter struct{}

func New() introspect.Introspecter {
	return &introspecter{}
}

func (i *introspecter) Introspect(_ context.Context, _ *sql.DB) (*schema.Database, error) {
	return nil, nil
}
