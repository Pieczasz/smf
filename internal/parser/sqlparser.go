// Package parser provides implementation to parse SQL schema dumps.
// For now, we support only MySQL syntax. But upcoming features will have psql, sqlite, and more.
package parser

import (
	"smf/internal/core"
	"smf/internal/parser/mysql"
)

type SQLParser struct {
	// TODO: Add support for other SQL dialects like PostgreSQL, SQLite, etc.
	mysqlParser *mysql.Parser
}

func NewSQLParser() *SQLParser {
	// TODO: make sure to later distinguish between which SQL dialect is being used.
	return &SQLParser{
		mysqlParser: mysql.NewParser(),
	}
}

func (p *SQLParser) ParseSchema(sql string) (*core.Database, error) {
	// TODO: make sure to later distinguish between which SQL dialect is being used.
	return p.mysqlParser.Parse(sql)
}
