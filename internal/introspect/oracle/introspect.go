package oracle

import (
	"context"
	"database/sql"

	"smf/internal/core"
	"smf/internal/introspect"
)

func init() {
	introspect.Register(core.DialectOracle, New)
}

type oracleIntrospecter struct{}

func New() introspect.Introspecter {
	return &oracleIntrospecter{}
}

func (i *oracleIntrospecter) Introspect(_ context.Context, _ *sql.DB) (*core.Database, error) {
	return nil, nil
}
