// Package mysql contains introspect implementation for MySQL, MariaDB and TiDB dialects,
// since they support the same binary, it detects which dialect it is and uses sql pool connection
// to get all desired database for core.Database struct.
package mysql

import (
	"context"
	"database/sql"

	"smf/internal/core"
	"smf/internal/introspect"
)

func init() {
	introspect.Register(core.DialectMySQL, New)
	introspect.Register(core.DialectMariaDB, New)
	introspect.Register(core.DialectTiDB, New)
}

type introspecter struct{}

type introspectCtx struct {
	dialect core.Dialect
	version string
	db      *sql.DB
	ctx     context.Context
}

func New() introspect.Introspecter {
	return &introspecter{}
}

func (i *introspecter) Introspect(ctx context.Context, db *sql.DB) (*core.Database, error) {
	d := new(core.Database)
	err := db.QueryRowContext(ctx, "SELECT DATABASE()").Scan(&d.Name)
	if err != nil {
		return nil, err
	}

	dialect, version, err := detectDialect(ctx, db)
	ic := &introspectCtx{
		dialect: dialect,
		version: version,
		db:      db,
		ctx:     ctx,
	}
	if err != nil {
		return nil, err
	}
	d.Dialect = &dialect

	err = introspectTables(ic, d)
	if err != nil {
		return nil, err
	}

	return d, nil
}
