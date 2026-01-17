package dialect

import (
	"smf/internal/core"
	diff2 "smf/internal/diff"
	"smf/internal/migration"
)

type Type string

const (
	MySQL      Type = "mysql"
	PostgreSQL Type = "postgresql"
	SQLite     Type = "sqlite"
	MSSQL      Type = "mssql"
	Oracle     Type = "oracle"
)

type Generator interface {
	GenerateMigration(diff *diff2.SchemaDiff) *migration.Migration
	GenerateMigrationWithOptions(diff *diff2.SchemaDiff, opts MigrationOptions) *migration.Migration
	GenerateCreateTable(table *core.Table) (statement string, fkStatements []string)
	GenerateDropTable(table *core.Table) string
	GenerateAlterTable(diff *diff2.TableDiff) []string
	QuoteIdentifier(name string) string
	QuoteString(value string) string
}

type Parser interface {
	Parse(sql string) (*core.Database, error)
}

type Dialect interface {
	Name() Type
	Generator() Generator
	Parser() Parser
}

var registry = map[Type]func() Dialect{}

func RegisterDialect(d Type, ctor func() Dialect) {
	registry[d] = ctor
}

func GetDialect(d Type) Dialect {
	if ctor, ok := registry[d]; ok {
		return ctor()
	}
	if ctor, ok := registry[MySQL]; ok {
		return ctor()
	}
	return nil
}

type BreakingChangeDetector interface {
	DetectBreakingChanges(schemaDiff *diff2.SchemaDiff) []diff2.BreakingChange
}

type MigrationOptions struct {
	Dialect              Type
	IncludeDrops         bool
	IncludeUnsafe        bool
	TransactionMode      TransactionMode
	PreserveForeignKeys  bool
	DeferForeignKeyCheck bool
}

type TransactionMode int

const (
	TransactionNone TransactionMode = iota
	TransactionSingle
	TransactionPerStatement
)

func DefaultMigrationOptions(dialect Type) MigrationOptions {
	return MigrationOptions{
		Dialect:              dialect,
		IncludeDrops:         true,
		IncludeUnsafe:        false,
		TransactionMode:      TransactionSingle,
		PreserveForeignKeys:  true,
		DeferForeignKeyCheck: true,
	}
}
