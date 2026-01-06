package dialect

import (
	"schemift/core"
)

type Generator interface {
	GenerateMigration(diff *core.SchemaDiff) *core.Migration
	GenerateMigrationWithOptions(diff *core.SchemaDiff, opts core.MigrationOptions) *core.Migration
	GenerateCreateTable(table *core.Table) (statement string, fkStatements []string)
	GenerateDropTable(table *core.Table) string
	GenerateAlterTable(diff *core.TableDiff) []string
	QuoteIdentifier(name string) string
	QuoteString(value string) string
}

type Parser interface {
	Parse(sql string) (*core.Database, error)
}

type Dialect interface {
	Name() core.Dialect
	Generator() Generator
	Parser() Parser
}

func GetDialect(d core.Dialect) Dialect {
	switch d {
	case core.DialectMySQL:
		return NewMySQLDialect()
	default:
		return NewMySQLDialect()
	}
}
