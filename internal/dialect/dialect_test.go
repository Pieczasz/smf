package dialect

import (
	"maps"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"smf/internal/core"
	"smf/internal/diff"
	"smf/internal/migration"
)

type mockGenerator struct{}

func (m *mockGenerator) GenerateMigration(d *diff.SchemaDiff) *migration.Migration {
	return &migration.Migration{}
}

func (m *mockGenerator) GenerateMigrationWithOptions(d *diff.SchemaDiff, opts MigrationOptions) *migration.Migration {
	return &migration.Migration{}
}

func (m *mockGenerator) GenerateCreateTable(table *core.Table) (statement string, fkStatements []string) {
	return "CREATE TABLE", nil
}

func (m *mockGenerator) GenerateDropTable(table *core.Table) string {
	return "DROP TABLE"
}

func (m *mockGenerator) GenerateAlterTable(d *diff.TableDiff) []string {
	return []string{"ALTER TABLE"}
}

func (m *mockGenerator) QuoteIdentifier(name string) string {
	return "`" + name + "`"
}

func (m *mockGenerator) QuoteString(value string) string {
	return "'" + value + "'"
}

type mockParser struct{}

func (m *mockParser) Parse(sql string) (*core.Database, error) {
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

func TestRegisterDialect(t *testing.T) {
	originalRegistry := make(map[Type]func() Dialect)
	maps.Copy(originalRegistry, registry)
	defer func() {
		registry = originalRegistry
	}()

	registry = make(map[Type]func() Dialect)

	testDialectType := Type("test_dialect")
	ctor := func() Dialect {
		return &mockDialect{name: testDialectType}
	}

	RegisterDialect(testDialectType, ctor)

	assert.Contains(t, registry, testDialectType)

	dialect := registry[testDialectType]()
	require.NotNil(t, dialect)
	assert.Equal(t, testDialectType, dialect.Name())
}

func TestRegisterDialectOverwrite(t *testing.T) {
	originalRegistry := make(map[Type]func() Dialect)
	maps.Copy(originalRegistry, registry)
	defer func() {
		registry = originalRegistry
	}()

	registry = make(map[Type]func() Dialect)

	testDialectType := Type("overwrite_dialect")

	firstCtor := func() Dialect {
		return &mockDialect{name: Type("first")}
	}
	RegisterDialect(testDialectType, firstCtor)

	secondCtor := func() Dialect {
		return &mockDialect{name: Type("second")}
	}
	RegisterDialect(testDialectType, secondCtor)

	dialect := registry[testDialectType]()
	require.NotNil(t, dialect)
	assert.Equal(t, Type("second"), dialect.Name())
}

func TestGetDialectExistingDialect(t *testing.T) {
	originalRegistry := make(map[Type]func() Dialect)
	maps.Copy(originalRegistry, registry)
	defer func() {
		registry = originalRegistry
	}()

	registry = make(map[Type]func() Dialect)

	testDialectType := Type("get_test_dialect")
	RegisterDialect(testDialectType, func() Dialect {
		return &mockDialect{name: testDialectType}
	})

	dialect := GetDialect(testDialectType)

	require.NotNil(t, dialect)
	assert.Equal(t, testDialectType, dialect.Name())
}

func TestGetDialectFallbackToMySQL(t *testing.T) {
	originalRegistry := make(map[Type]func() Dialect)
	maps.Copy(originalRegistry, registry)
	defer func() {
		registry = originalRegistry
	}()

	registry = make(map[Type]func() Dialect)

	RegisterDialect(MySQL, func() Dialect {
		return &mockDialect{name: MySQL}
	})

	dialect := GetDialect(PostgreSQL)

	require.NotNil(t, dialect)
	assert.Equal(t, MySQL, dialect.Name())
}

func TestGetDialectNoDialectsRegistered(t *testing.T) {
	originalRegistry := make(map[Type]func() Dialect)
	maps.Copy(originalRegistry, registry)
	defer func() {
		registry = originalRegistry
	}()

	registry = make(map[Type]func() Dialect)

	dialect := GetDialect(MySQL)

	assert.Nil(t, dialect)
}

func TestGetDialectNonExistentNoMySQLFallback(t *testing.T) {
	originalRegistry := make(map[Type]func() Dialect)
	maps.Copy(originalRegistry, registry)
	defer func() {
		registry = originalRegistry
	}()

	registry = make(map[Type]func() Dialect)
	RegisterDialect(PostgreSQL, func() Dialect {
		return &mockDialect{name: PostgreSQL}
	})

	dialect := GetDialect(SQLite)

	assert.Nil(t, dialect)
}

func TestDefaultMigrationOptions(t *testing.T) {
	tests := []struct {
		name     string
		dialect  Type
		expected MigrationOptions
	}{
		{
			name:    "MySQL dialect",
			dialect: MySQL,
			expected: MigrationOptions{
				Dialect:              MySQL,
				IncludeDrops:         true,
				IncludeUnsafe:        false,
				TransactionMode:      TransactionSingle,
				PreserveForeignKeys:  true,
				DeferForeignKeyCheck: true,
			},
		},
		{
			name:    "PostgreSQL dialect",
			dialect: PostgreSQL,
			expected: MigrationOptions{
				Dialect:              PostgreSQL,
				IncludeDrops:         true,
				IncludeUnsafe:        false,
				TransactionMode:      TransactionSingle,
				PreserveForeignKeys:  true,
				DeferForeignKeyCheck: true,
			},
		},
		{
			name:    "SQLite dialect",
			dialect: SQLite,
			expected: MigrationOptions{
				Dialect:              SQLite,
				IncludeDrops:         true,
				IncludeUnsafe:        false,
				TransactionMode:      TransactionSingle,
				PreserveForeignKeys:  true,
				DeferForeignKeyCheck: true,
			},
		},
		{
			name:    "MSSQL dialect",
			dialect: MSSQL,
			expected: MigrationOptions{
				Dialect:              MSSQL,
				IncludeDrops:         true,
				IncludeUnsafe:        false,
				TransactionMode:      TransactionSingle,
				PreserveForeignKeys:  true,
				DeferForeignKeyCheck: true,
			},
		},
		{
			name:    "Oracle dialect",
			dialect: Oracle,
			expected: MigrationOptions{
				Dialect:              Oracle,
				IncludeDrops:         true,
				IncludeUnsafe:        false,
				TransactionMode:      TransactionSingle,
				PreserveForeignKeys:  true,
				DeferForeignKeyCheck: true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := DefaultMigrationOptions(tt.dialect)

			assert.Equal(t, tt.expected.Dialect, opts.Dialect)
			assert.Equal(t, tt.expected.IncludeDrops, opts.IncludeDrops)
			assert.Equal(t, tt.expected.IncludeUnsafe, opts.IncludeUnsafe)
			assert.Equal(t, tt.expected.TransactionMode, opts.TransactionMode)
			assert.Equal(t, tt.expected.PreserveForeignKeys, opts.PreserveForeignKeys)
			assert.Equal(t, tt.expected.DeferForeignKeyCheck, opts.DeferForeignKeyCheck)
		})
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

	assert.NotNil(t, g.GenerateMigration(nil))
	assert.NotNil(t, g.GenerateMigrationWithOptions(nil, MigrationOptions{}))

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
