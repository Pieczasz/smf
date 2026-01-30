package mysql

import (
	"strings"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"

	"smf/internal/core"
)

func (p *Parser) parseColumns(cols []*ast.ColumnDef, table *core.Table) {
	for _, colDef := range cols {
		col := &core.Column{
			Name:     colDef.Name.Name.O,
			TypeRaw:  colDef.Tp.String(),
			Type:     core.NormalizeDataType(colDef.Tp.String()),
			Nullable: true,
			Collate:  colDef.Tp.GetCollate(),
			Charset:  colDef.Tp.GetCharset(),
		}

		for _, opt := range colDef.Options {
			switch opt.Tp {
			case ast.ColumnOptionNotNull:
				col.Nullable = false
			case ast.ColumnOptionNull:
				col.Nullable = true
			case ast.ColumnOptionPrimaryKey:
				col.PrimaryKey = true
				col.Nullable = false
			case ast.ColumnOptionAutoIncrement:
				col.AutoIncrement = true
			case ast.ColumnOptionDefaultValue:
				col.DefaultValue = p.exprToString(opt.Expr)
			case ast.ColumnOptionOnUpdate:
				col.OnUpdate = p.exprToString(opt.Expr)
			case ast.ColumnOptionUniqKey:
				table.Constraints = append(table.Constraints, &core.Constraint{
					Type:    core.ConstraintUnique,
					Columns: []string{col.Name},
				})
			case ast.ColumnOptionComment:
				if s := p.exprToString(opt.Expr); s != nil {
					col.Comment = *s
				}
			case ast.ColumnOptionCollate:
				if s := p.exprToString(opt.Expr); s != nil {
					col.Collate = *s
				} else if opt.StrValue != "" {
					col.Collate = opt.StrValue
				}
			case ast.ColumnOptionFulltext:
				table.Indexes = append(table.Indexes, &core.Index{
					Columns: []core.IndexColumn{{Name: col.Name}},
					Unique:  false,
					Type:    core.IndexTypeFullText,
				})
			case ast.ColumnOptionCheck:
				if s := p.exprToString(opt.Expr); s != nil {
					table.Constraints = append(table.Constraints, &core.Constraint{
						Type:            core.ConstraintCheck,
						Columns:         []string{col.Name},
						CheckExpression: *s,
					})
				}
			case ast.ColumnOptionReference:
				c := &core.Constraint{
					Type:            core.ConstraintForeignKey,
					Columns:         []string{col.Name},
					ReferencedTable: opt.Refer.Table.Name.O,
				}
				for _, spec := range opt.Refer.IndexPartSpecifications {
					if spec.Column != nil {
						c.ReferencedColumns = append(c.ReferencedColumns, spec.Column.Name.O)
					}
				}
				if opt.Refer.OnDelete != nil {
					c.OnDelete = core.ReferentialAction(opt.Refer.OnDelete.ReferOpt.String())
				}
				if opt.Refer.OnUpdate != nil {
					c.OnUpdate = core.ReferentialAction(opt.Refer.OnUpdate.ReferOpt.String())
				}
				table.Constraints = append(table.Constraints, c)
			case ast.ColumnOptionGenerated:
				col.IsGenerated = true
				if opt.Expr != nil {
					if s := p.exprToString(opt.Expr); s != nil {
						col.GenerationExpression = *s
					}
				}
				if opt.Stored {
					col.GenerationStorage = core.GenerationStored
				} else {
					col.GenerationStorage = core.GenerationVirtual
				}
			case ast.ColumnOptionColumnFormat:
				col.ColumnFormat = opt.StrValue
			case ast.ColumnOptionStorage:
				col.Storage = opt.StrValue
			case ast.ColumnOptionAutoRandom:
				col.AutoRandom = uint64(opt.AutoRandOpt.ShardBits)
			case ast.ColumnOptionSecondaryEngineAttribute:
				col.SecondaryEngineAttribute = opt.StrValue
			case ast.ColumnOptionNoOption:
			}
		}
		table.Columns = append(table.Columns, col)
		if col.PrimaryKey {
			p.ensurePrimaryKeyColumn(table, col.Name)
		}
	}
}

func (p *Parser) ensurePrimaryKeyColumn(table *core.Table, colName string) {
	colName = strings.TrimSpace(colName)
	if colName == "" {
		return
	}

	var pk *core.Constraint
	for _, c := range table.Constraints {
		if c == nil {
			continue
		}
		if c.Type == core.ConstraintPrimaryKey {
			pk = c
			break
		}
	}
	if pk == nil {
		pk = &core.Constraint{
			Name:    "PRIMARY",
			Type:    core.ConstraintPrimaryKey,
			Columns: []string{},
		}
		table.Constraints = append(table.Constraints, pk)
	}
	if strings.TrimSpace(pk.Name) == "" {
		pk.Name = "PRIMARY"
	}

	for _, existing := range pk.Columns {
		if strings.EqualFold(existing, colName) {
			if col := table.FindColumn(colName); col != nil {
				col.PrimaryKey = true
				col.Nullable = false
			}
			return
		}
	}
	pk.Columns = append(pk.Columns, colName)
	if col := table.FindColumn(colName); col != nil {
		col.PrimaryKey = true
		col.Nullable = false
	}
}

func (p *Parser) parseConstraints(constraints []*ast.Constraint, table *core.Table) {
	for _, constraint := range constraints {
		columns := make([]string, 0, len(constraint.Keys))
		indexCols := make([]core.IndexColumn, 0, len(constraint.Keys))
		for _, key := range constraint.Keys {
			columns = append(columns, key.Column.Name.O)
			indexCols = append(indexCols, core.IndexColumn{
				Name:   key.Column.Name.O,
				Length: key.Length,
			})
		}

		switch constraint.Tp {
		case ast.ConstraintPrimaryKey:
			for _, colName := range columns {
				p.ensurePrimaryKeyColumn(table, colName)
			}
			if pk := table.PrimaryKey(); pk != nil {
				pk.Name = "PRIMARY"
				pk.Columns = columns
			}

		case ast.ConstraintUniq, ast.ConstraintUniqKey, ast.ConstraintUniqIndex:
			table.Constraints = append(table.Constraints, &core.Constraint{
				Name:    constraint.Name,
				Type:    core.ConstraintUnique,
				Columns: columns,
			})

		case ast.ConstraintForeignKey:
			c := &core.Constraint{
				Name:            constraint.Name,
				Type:            core.ConstraintForeignKey,
				Columns:         columns,
				ReferencedTable: constraint.Refer.Table.Name.O,
			}
			for _, spec := range constraint.Refer.IndexPartSpecifications {
				if spec.Column != nil {
					c.ReferencedColumns = append(c.ReferencedColumns, spec.Column.Name.O)
				}
			}
			if constraint.Refer.OnDelete != nil {
				c.OnDelete = core.ReferentialAction(constraint.Refer.OnDelete.ReferOpt.String())
			}
			if constraint.Refer.OnUpdate != nil {
				c.OnUpdate = core.ReferentialAction(constraint.Refer.OnUpdate.ReferOpt.String())
			}
			table.Constraints = append(table.Constraints, c)

		case ast.ConstraintIndex, ast.ConstraintKey:
			table.Indexes = append(table.Indexes, &core.Index{
				Name:    constraint.Name,
				Columns: indexCols,
				Unique:  false,
				Type:    core.IndexTypeBTree,
			})

		case ast.ConstraintFulltext:
			table.Indexes = append(table.Indexes, &core.Index{
				Name:    constraint.Name,
				Columns: indexCols,
				Unique:  false,
				Type:    core.IndexTypeFullText,
			})

		case ast.ConstraintCheck:
			c := &core.Constraint{
				Name:    constraint.Name,
				Type:    core.ConstraintCheck,
				Columns: columns,
			}
			if constraint.Expr != nil {
				if s := p.exprToString(constraint.Expr); s != nil {
					c.CheckExpression = *s
				}
			}
			table.Constraints = append(table.Constraints, c)

		case ast.ConstraintVector, ast.ConstraintColumnar:
			table.Indexes = append(table.Indexes, &core.Index{
				Name:    constraint.Name,
				Columns: indexCols,
				Unique:  false,
				Type:    core.IndexTypeBTree,
			})
		case ast.ConstraintNoConstraint:
		}
	}
}

func (p *Parser) exprToString(expr ast.ExprNode) *string {
	if expr == nil {
		return nil
	}

	var sb strings.Builder
	restoreCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)
	if err := expr.Restore(restoreCtx); err != nil {
		return nil
	}
	s := strings.TrimSpace(sb.String())

	if unquoted, ok := tryUnquoteSQLStringLiteral(s); ok {
		return &unquoted
	}

	return &s
}

func tryUnquoteSQLStringLiteral(s string) (string, bool) {
	s = strings.TrimSpace(s)
	if len(s) < 2 || s[len(s)-1] != '\'' {
		return "", false
	}

	if s[0] == '\'' {
		return strings.ReplaceAll(s[1:len(s)-1], "''", "'"), true
	}

	q := strings.IndexByte(s, '\'')
	if q <= 0 {
		return "", false
	}
	prefix := strings.TrimSpace(s[:q])
	if !isSQLStringIntroducer(prefix) {
		return "", false
	}
	inner := s[q+1 : len(s)-1]
	return strings.ReplaceAll(inner, "''", "'"), true
}

func isSQLStringIntroducer(prefix string) bool {
	if prefix == "" {
		return false
	}
	if strings.EqualFold(prefix, "N") {
		return true
	}
	if !strings.HasPrefix(prefix, "_") || len(prefix) == 1 {
		return false
	}
	for _, r := range prefix[1:] {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9':
		case r == '_':
		default:
			return false
		}
	}
	return true
}
