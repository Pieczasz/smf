// Package mysql contains introspect implementation for MySQL, MariaDB, and TiDB dialects,
// since they support the same binary, it detects which dialect it is and uses SQL pool connection
// to get all desired databases for core.Database struct.
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
	ctx     context.Context
	dialect core.Dialect
	version string
	db      *sql.DB
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
	if err != nil {
		return nil, err
	}
	d.Dialect = dialect

	ic := &introspectCtx{
		dialect: dialect,
		version: version,
		db:      db,
		ctx:     ctx,
	}

	err = introspectTables(ic, d)
	if err != nil {
		return nil, err
	}

	return d, nil
}
