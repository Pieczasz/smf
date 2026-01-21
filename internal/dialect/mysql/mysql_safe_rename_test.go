package mysql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"smf/internal/core"
	"smf/internal/dialect"
	"smf/internal/diff"
)

func TestMySQLSafeModeUsesChangeColumnForRename(t *testing.T) {
	oldDB := &core.Database{Tables: []*core.Table{{
		Name:    "users",
		Columns: []*core.Column{{Name: "password_hash", TypeRaw: "VARBINARY(60)", Type: core.NormalizeDataType("VARBINARY(60)"), Nullable: false}},
	}}}

	newDB := &core.Database{Tables: []*core.Table{{
		Name:    "users",
		Columns: []*core.Column{{Name: "password_digest", TypeRaw: "VARBINARY(72)", Type: core.NormalizeDataType("VARBINARY(72)"), Nullable: false}},
	}}}

	d := diff.Diff(oldDB, newDB, diff.DefaultOptions())
	require.NotNil(t, d)

	gen := NewMySQLDialect().Generator()
	opts := dialect.DefaultMigrationOptions(dialect.MySQL)
	opts.IncludeUnsafe = false

	mig := gen.GenerateMigrationWithOptions(d, opts)
	require.NotNil(t, mig)

	out := mig.String()
	assert.Contains(t, out, "CHANGE COLUMN")
	assert.Contains(t, out, "password_hash")
	assert.Contains(t, out, "password_digest")
	assert.NotContains(t, out, "DROP COLUMN `password_hash`")
}
