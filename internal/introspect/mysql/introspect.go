// Package mysql contains introspect implementation for MySQL, MariaDB, and TiDB dialects,
// since they support the same binary, it detects which dialect it is and uses SQL pool connection
// to get all desired databases for schema.Database struct.
// NOTE: refer to https://dev.mysql.com/doc/refman/8.4/en/create-table.html
package mysql

import (
	"context"
	"database/sql"

	"smf/internal/introspect"
	"smf/internal/schema"
)

func init() {
	introspect.Register(schema.DialectMySQL, New)
	introspect.Register(schema.DialectMariaDB, New)
	introspect.Register(schema.DialectTiDB, New)
}

type introspecter struct{}

type introspectCtx struct {
	ctx     context.Context
	dialect schema.Dialect
	version string
	db      *sql.DB
}

func New() introspect.Introspecter {
	return &introspecter{}
}

func (i *introspecter) Introspect(ctx context.Context, db *sql.DB) (*schema.Database, error) {
	d := new(schema.Database)
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
