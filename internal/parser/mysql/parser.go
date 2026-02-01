// Package mysql inside parser, provides implementation to parse MySQL schema dumps.
// It uses TiDB's parser, so we support both MySQL syntax and TiDB-specific options.
package mysql

import (
	"fmt"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"

	"smf/internal/core"
)

type Parser struct {
	p *parser.Parser
}

func NewParser() *Parser {
	return &Parser{p: parser.New()}
}

func (p *Parser) Parse(sql string) (*core.Database, error) {
	// NOTE: this can be parallelized, it can help if schema dumps are big.
	stmtNodes, _, err := p.p.Parse(sql, "", "")
	if err != nil {
		return nil, fmt.Errorf("SQL parse error: %w", err)
	}

	db := &core.Database{Tables: []*core.Table{}}
	for _, stmt := range stmtNodes {
		if create, ok := stmt.(*ast.CreateTableStmt); ok {
			tableName := create.Table.Name.O
			table, err := p.convertCreateTable(create)
			if err != nil {
				return nil, fmt.Errorf("failed to parse table %q: %w", tableName, err)
			}
			db.Tables = append(db.Tables, table)
		}
	}

	if err := db.Validate(); err != nil {
		return nil, fmt.Errorf("schema validation failed: %w", err)
	}

	return db, nil
}

func (p *Parser) convertCreateTable(stmt *ast.CreateTableStmt) (*core.Table, error) {
	table := &core.Table{
		Name:    stmt.Table.Name.O,
		Columns: []*core.Column{},
	}

	p.parseTableOptions(stmt.Options, table)
	p.parseColumns(stmt.Cols, table)
	p.parseConstraints(stmt.Constraints, table)

	return table, nil
}
