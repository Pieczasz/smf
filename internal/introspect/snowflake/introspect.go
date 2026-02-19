package snowflake

import (
	"context"
	"database/sql"

	"smf/internal/core"
	"smf/internal/introspect"
)

func init() {
	introspect.Register(core.DialectSnowflake, New)
}

type introspecter struct{}

func New() introspect.Introspecter {
	return &introspecter{}
}

func (i *introspecter) Introspect(_ context.Context, _ *sql.DB) (*core.Database, error) {
	return nil, nil
}
