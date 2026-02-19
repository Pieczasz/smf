package mssql

import (
	"context"
	"database/sql"

	"smf/internal/core"
	"smf/internal/introspect"
)

func init() {
	introspect.Register(core.DialectMSSQL, New)
}

type mssqlIntrospecter struct{}

func New() introspect.Introspecter {
	return &mssqlIntrospecter{}
}

func (i *mssqlIntrospecter) Introspect(_ context.Context, _ *sql.DB) (*core.Database, error) {
	return nil, nil
}
