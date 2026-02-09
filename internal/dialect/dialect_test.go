package dialect

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"smf/internal/core"
	"smf/internal/diff"
	"smf/internal/migration"
)

type mockGenerator struct{}

func (m *mockGenerator) GenerateMigration(_ *diff.SchemaDiff, _ MigrationOptions) *migration.Migration {
	return &migration.Migration{}
}

func (m *mockGenerator) GenerateCreateTable(_ *core.Table) (statement string, fkStatements []string) {
	return "CREATE TABLE", nil
}

func (m *mockGenerator) GenerateDropTable(_ *core.Table) string {
	return "DROP TABLE"
}

func (m *mockGenerator) GenerateAlterTable(_ *diff.TableDiff) []string {
	return []string{"ALTER TABLE"}
}

func (m *mockGenerator) QuoteIdentifier(name string) string {
	return "`" + name + "`"
}

func (m *mockGenerator) QuoteString(value string) string {
	return "'" + value + "'"
}

type mockParser struct{}

func (m *mockParser) Parse(_ string) (*core.Database, error) {
	return &core.Database{}, nil
}

type mockDialect struct {
	name Type
}

func (m *mockDialect) Name() Type {
	return m.name
}

func (m *mockDialect) Generator() Generator {
	return &mockGenerator{}
}

func (m *mockDialect) Parser() Parser {
	return &mockParser{}
}

// withCleanRegistry saves the current registry, replaces it with an empty one,
// and returns a cleanup function that restores the original.
func withCleanRegistry(t *testing.T) {
	t.Helper()
	original := snapshotRegistry()
	resetRegistry(make(map[Type]func() Dialect))
	t.Cleanup(func() {
		resetRegistry(original)
	})
}

func TestRegisterDialect(t *testing.T) {
	withCleanRegistry(t)

	testDialectType := Type("test_dialect")
	ctor := func() Dialect {
		return &mockDialect{name: testDialectType}
	}

	RegisterDialect(testDialectType, ctor)

	d, err := GetDialect(testDialectType)
	require.NoError(t, err)
	require.NotNil(t, d)
	assert.Equal(t, testDialectType, d.Name())
}

func TestRegisterDialectOverwrite(t *testing.T) {
	withCleanRegistry(t)

	testDialectType := Type("overwrite_dialect")

	firstCtor := func() Dialect {
		return &mockDialect{name: Type("first")}
	}
	RegisterDialect(testDialectType, firstCtor)

	secondCtor := func() Dialect {
		return &mockDialect{name: Type("second")}
	}
	RegisterDialect(testDialectType, secondCtor)

	d, err := GetDialect(testDialectType)
	require.NoError(t, err)
	require.NotNil(t, d)
	assert.Equal(t, Type("second"), d.Name())
}

func TestGetDialectExistingDialect(t *testing.T) {
	withCleanRegistry(t)

	testDialectType := Type("get_test_dialect")
	RegisterDialect(testDialectType, func() Dialect {
		return &mockDialect{name: testDialectType}
	})

	d, err := GetDialect(testDialectType)

	require.NoError(t, err)
	require.NotNil(t, d)
	assert.Equal(t, testDialectType, d.Name())
}

func TestGetDialectUnregisteredReturnsError(t *testing.T) {
	withCleanRegistry(t)

	d, err := GetDialect(PostgreSQL)

	assert.Nil(t, d)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not registered")
}

func TestGetDialectNoDialectsRegistered(t *testing.T) {
	withCleanRegistry(t)

	d, err := GetDialect(MySQL)

	assert.Nil(t, d)
	require.Error(t, err)
}

func TestDefaultMigrationOptions(t *testing.T) {
	opts := DefaultMigrationOptions(MySQL)

	assert.Equal(t, MySQL, opts.Dialect)
	assert.True(t, opts.IncludeDrops)
	assert.False(t, opts.IncludeUnsafe)
	assert.Equal(t, TransactionSingle, opts.TransactionMode)
	assert.True(t, opts.PreserveForeignKeys)
	assert.True(t, opts.DeferForeignKeyCheck)
}

func TestDefaultMigrationOptionsDialectField(t *testing.T) {
	for _, d := range []Type{MySQL, PostgreSQL, SQLite, MSSQL, Oracle} {
		opts := DefaultMigrationOptions(d)
		assert.Equal(t, d, opts.Dialect)
	}
}

func TestTransactionModeConstants(t *testing.T) {
	assert.Equal(t, TransactionMode(0), TransactionNone)
	assert.Equal(t, TransactionMode(1), TransactionSingle)
	assert.Equal(t, TransactionMode(2), TransactionPerStatement)
}

func TestDialectTypeConstants(t *testing.T) {
	assert.Equal(t, Type("mysql"), MySQL)
	assert.Equal(t, Type("postgresql"), PostgreSQL)
	assert.Equal(t, Type("sqlite"), SQLite)
	assert.Equal(t, Type("mssql"), MSSQL)
	assert.Equal(t, Type("oracle"), Oracle)
}

func TestMockDialectImplementsInterface(t *testing.T) {
	var d Dialect = &mockDialect{name: MySQL}

	assert.Equal(t, MySQL, d.Name())
	assert.NotNil(t, d.Generator())
	assert.NotNil(t, d.Parser())
}

func TestMockGeneratorImplementsInterface(t *testing.T) {
	var g Generator = &mockGenerator{}

	assert.NotNil(t, g.GenerateMigration(nil, MigrationOptions{}))

	stmt, fks := g.GenerateCreateTable(nil)
	assert.Equal(t, "CREATE TABLE", stmt)
	assert.Nil(t, fks)

	assert.Equal(t, "DROP TABLE", g.GenerateDropTable(nil))
	assert.Equal(t, []string{"ALTER TABLE"}, g.GenerateAlterTable(nil))
	assert.Equal(t, "`test`", g.QuoteIdentifier("test"))
	assert.Equal(t, "'value'", g.QuoteString("value"))
}

func TestMockParserImplementsInterface(t *testing.T) {
	var p Parser = &mockParser{}

	db, err := p.Parse("SELECT 1")
	assert.NoError(t, err)
	assert.NotNil(t, db)
}
