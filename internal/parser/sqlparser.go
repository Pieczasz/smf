// Package parser provides implementation to parse SQL schema dumps.
// Currently supports MySQL syntax. PostgreSQL support planned for M5 milestone.
package parser

import (
	"smf/internal/core"
	"smf/internal/parser/mysql"
)

// SQLParser is a facade that delegates to dialect-specific parsers.
// Currently only MySQL is supported; additional dialects will be added
// by creating new subpackages (e.g., parser/postgres).
type SQLParser struct {
	mysqlParser *mysql.Parser
}

// NewSQLParser creates a new SQL parser. Currently defaults to MySQL.
// Future: Accept dialect parameter to select appropriate parser.
func NewSQLParser() *SQLParser {
	return &SQLParser{
		mysqlParser: mysql.NewParser(),
	}
}

// ParseSchema parses a SQL schema dump and returns a Database representation.
// Currently uses MySQL parser; future versions will auto-detect or accept dialect.
func (p *SQLParser) ParseSchema(sql string) (*core.Database, error) {
	return p.mysqlParser.Parse(sql)
}
