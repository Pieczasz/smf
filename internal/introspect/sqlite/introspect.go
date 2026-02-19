package sqlite

import (
	"context"
	"database/sql"

	"smf/internal/core"
	"smf/internal/introspect"
)

func init() {
	introspect.Register(core.DialectSQLite, New)
}

type sqliteIntrospecter struct{}

func New() introspect.Introspecter {
	return &sqliteIntrospecter{}
}

func (i *sqliteIntrospecter) Introspect(_ context.Context, _ *sql.DB) (*core.Database, error) {
	return nil, nil
}
