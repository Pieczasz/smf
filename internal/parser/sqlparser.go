package parser

import (
	"fmt"
	"strings"

	"schemift/internal/core"

	"github.com/xwb1989/sqlparser"
)

type SQLParser struct{}

func NewSQLParser() *SQLParser {
	return &SQLParser{}
}

func (p *SQLParser) ParseSchema(sql string) (*core.Database, error) {
	db := &core.Database{
		Tables: []*core.Table{},
	}

	pieces, err := sqlparser.SplitStatementToPieces(sql)
	if err != nil {
		return nil, fmt.Errorf("failed to split statements: %w", err)
	}

	for _, piece := range pieces {
		piece = strings.TrimSpace(piece)
		if piece == "" {
			continue
		}

		stmt, err := sqlparser.Parse(piece)
		if err != nil {
			return nil, fmt.Errorf("parse error in statement '%s': %w", piece, err)
		}

		createTable, ok := stmt.(*sqlparser.CreateTable)
		if !ok {
			continue
		}

		table := &core.Table{
			Name:        createTable.Table.Name.String(),
			Columns:     []*core.Column{},
			Constraints: []*core.Constraint{},
			Indexes:     []*core.Index{},
		}

		for _, col := range createTable.Columns {
			c := &core.Column{
				Name:          col.Name.String(),
				TypeRaw:       col.Type.Type,
				Nullable:      !col.Type.NotNull,
				AutoIncrement: col.Type.Autoincrement,
			}
			if col.Type.Default != nil {
				val := string(col.Type.Default.Val)
				c.DefaultValue = &val
			}
			table.Columns = append(table.Columns, c)
		}

		for _, idx := range createTable.Indexes {
			if idx.Info.Primary {
				pkCols := extractColNames(idx.Columns)
				for _, c := range table.Columns {
					for _, pkName := range pkCols {
						if c.Name == pkName {
							c.PrimaryKey = true
						}
					}
				}
				table.Constraints = append(table.Constraints, &core.Constraint{
					Name:    "PRIMARY",
					Type:    "PRIMARY_KEY",
					Columns: pkCols,
				})
			} else if idx.Info.Unique {
				table.Constraints = append(table.Constraints, &core.Constraint{
					Name:    idx.Info.Name.String(),
					Type:    "UNIQUE",
					Columns: extractColNames(idx.Columns),
				})
			} else {
				table.Indexes = append(table.Indexes, &core.Index{
					Name:    idx.Info.Name.String(),
					Columns: extractColNames(idx.Columns),
					Unique:  false,
				})
			}
		}

		db.Tables = append(db.Tables, table)
	}

	return db, nil
}

func extractColNames(idxCols []*sqlparser.IndexColumn) []string {
	names := make([]string, len(idxCols))
	for i, col := range idxCols {
		names[i] = col.Column.String()
	}
	return names
}
