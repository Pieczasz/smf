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
		col := newColumnFromDef(colDef)
		for _, opt := range colDef.Options {
			p.applyColumnOption(table, col, opt)
		}
		table.Columns = append(table.Columns, col)
		if col.PrimaryKey {
			p.ensurePrimaryKeyColumn(table, col.Name)
		}
	}
}

func newColumnFromDef(colDef *ast.ColumnDef) *core.Column {
	typeRaw := colDef.Tp.String()
	return &core.Column{
		Name:     colDef.Name.Name.O,
		TypeRaw:  typeRaw,
		Type:     core.NormalizeDataType(typeRaw),
		Nullable: true,
		Collate:  colDef.Tp.GetCollate(),
		Charset:  colDef.Tp.GetCharset(),
	}
}

// TODO: refactor this logic to remove nolint comment
//
//nolint:revive // Large switch needed for AST option mapping
func (p *Parser) applyColumnOption(table *core.Table, col *core.Column, opt *ast.ColumnOption) {
	if opt == nil {
		return
	}

	switch opt.Tp {
	case ast.ColumnOptionNotNull:
		col.Nullable = false
	case ast.ColumnOptionNull:
		col.Nullable = true
	case ast.ColumnOptionPrimaryKey:
		p.applyPrimaryKeyOption(col)
	case ast.ColumnOptionAutoIncrement:
		col.AutoIncrement = true
	case ast.ColumnOptionDefaultValue:
		col.DefaultValue = p.exprToString(opt.Expr)
	case ast.ColumnOptionOnUpdate:
		col.OnUpdate = p.exprToString(opt.Expr)
	case ast.ColumnOptionUniqKey:
		p.addUniqueConstraintForColumn(table, col.Name)
	case ast.ColumnOptionComment:
		p.applyColumnCommentOption(col, opt)
	case ast.ColumnOptionCollate:
		p.applyColumnCollateOption(col, opt)
	case ast.ColumnOptionFulltext:
		p.addFulltextIndexForColumn(table, col.Name)
	case ast.ColumnOptionCheck:
		p.addCheckConstraintForColumn(table, col.Name, opt)
	case ast.ColumnOptionReference:
		p.addInlineForeignKeyConstraint(table, col.Name, opt)
	case ast.ColumnOptionGenerated:
		p.applyGeneratedColumnOption(col, opt)
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

func (p *Parser) applyPrimaryKeyOption(col *core.Column) {
	col.PrimaryKey = true
	col.Nullable = false
}

func (p *Parser) addUniqueConstraintForColumn(table *core.Table, colName string) {
	table.Constraints = append(table.Constraints, &core.Constraint{
		Type:    core.ConstraintUnique,
		Columns: []string{colName},
	})
}

func (p *Parser) addFulltextIndexForColumn(table *core.Table, colName string) {
	table.Indexes = append(table.Indexes, &core.Index{
		Columns: []core.IndexColumn{{Name: colName}},
		Unique:  false,
		Type:    core.IndexTypeFullText,
	})
}

func (p *Parser) applyColumnCommentOption(col *core.Column, opt *ast.ColumnOption) {
	if s := p.exprToString(opt.Expr); s != nil {
		col.Comment = *s
	}
}

func (p *Parser) applyColumnCollateOption(col *core.Column, opt *ast.ColumnOption) {
	if s := p.exprToString(opt.Expr); s != nil {
		col.Collate = *s
		return
	}
	if opt.StrValue != "" {
		col.Collate = opt.StrValue
	}
}

func (p *Parser) addCheckConstraintForColumn(table *core.Table, colName string, opt *ast.ColumnOption) {
	if s := p.exprToString(opt.Expr); s != nil {
		table.Constraints = append(table.Constraints, &core.Constraint{
			Type:            core.ConstraintCheck,
			Columns:         []string{colName},
			CheckExpression: *s,
		})
	}
}

func (p *Parser) addInlineForeignKeyConstraint(table *core.Table, colName string, opt *ast.ColumnOption) {
	c := &core.Constraint{
		Type:            core.ConstraintForeignKey,
		Columns:         []string{colName},
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
}

func (p *Parser) applyGeneratedColumnOption(col *core.Column, opt *ast.ColumnOption) {
	col.IsGenerated = true
	if opt.Expr != nil {
		if s := p.exprToString(opt.Expr); s != nil {
			col.GenerationExpression = *s
		}
	}
	if opt.Stored {
		col.GenerationStorage = core.GenerationStored
		return
	}
	col.GenerationStorage = core.GenerationVirtual
}

func (p *Parser) ensurePrimaryKeyColumn(table *core.Table, colName string) {
	colName = strings.TrimSpace(colName)
	if colName == "" {
		return
	}

	pk := p.findOrCreatePrimaryKeyConstraint(table)
	if p.columnAlreadyInPK(pk, colName) {
		p.markColumnAsPrimaryKey(table, colName)
		return
	}
	pk.Columns = append(pk.Columns, colName)
	p.markColumnAsPrimaryKey(table, colName)
}

func (p *Parser) findOrCreatePrimaryKeyConstraint(table *core.Table) *core.Constraint {
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
	return pk
}

func (p *Parser) columnAlreadyInPK(pk *core.Constraint, colName string) bool {
	for _, existing := range pk.Columns {
		if strings.EqualFold(existing, colName) {
			return true
		}
	}
	return false
}

func (p *Parser) markColumnAsPrimaryKey(table *core.Table, colName string) {
	if col := table.FindColumn(colName); col != nil {
		col.PrimaryKey = true
		col.Nullable = false
	}
}

func (p *Parser) parseConstraints(constraints []*ast.Constraint, table *core.Table) {
	for _, constraint := range constraints {
		columns, indexCols := constraintColumns(constraint)
		p.applyConstraint(table, constraint, columns, indexCols)
	}
}

func constraintColumns(constraint *ast.Constraint) ([]string, []core.IndexColumn) {
	if constraint == nil {
		return nil, nil
	}
	columns := make([]string, 0, len(constraint.Keys))
	indexCols := make([]core.IndexColumn, 0, len(constraint.Keys))
	for _, key := range constraint.Keys {
		columns = append(columns, key.Column.Name.O)
		indexCols = append(indexCols, core.IndexColumn{
			Name:   key.Column.Name.O,
			Length: key.Length,
		})
	}
	return columns, indexCols
}

func (p *Parser) applyConstraint(table *core.Table, constraint *ast.Constraint, columns []string, indexCols []core.IndexColumn) {
	if constraint == nil {
		return
	}

	switch constraint.Tp {
	case ast.ConstraintPrimaryKey:
		p.applyPrimaryKeyConstraint(table, columns)
	case ast.ConstraintUniq, ast.ConstraintUniqKey, ast.ConstraintUniqIndex:
		p.applyUniqueConstraint(table, constraint.Name, columns)
	case ast.ConstraintForeignKey:
		p.applyForeignKeyConstraint(table, constraint, columns)
	case ast.ConstraintIndex, ast.ConstraintKey:
		p.applyIndexConstraint(table, constraint.Name, indexCols, core.IndexTypeBTree)
	case ast.ConstraintFulltext:
		p.applyIndexConstraint(table, constraint.Name, indexCols, core.IndexTypeFullText)
	case ast.ConstraintCheck:
		p.applyCheckConstraint(table, constraint, columns)
	case ast.ConstraintVector, ast.ConstraintColumnar:
		p.applyIndexConstraint(table, constraint.Name, indexCols, core.IndexTypeBTree)
	case ast.ConstraintNoConstraint:
	}
}

func (p *Parser) applyPrimaryKeyConstraint(table *core.Table, columns []string) {
	for _, colName := range columns {
		p.ensurePrimaryKeyColumn(table, colName)
	}
	if pk := table.PrimaryKey(); pk != nil {
		pk.Name = "PRIMARY"
		pk.Columns = columns
	}
}

func (p *Parser) applyUniqueConstraint(table *core.Table, name string, columns []string) {
	table.Constraints = append(table.Constraints, &core.Constraint{
		Name:    name,
		Type:    core.ConstraintUnique,
		Columns: columns,
	})
}

func (p *Parser) applyForeignKeyConstraint(table *core.Table, constraint *ast.Constraint, columns []string) {
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
}

func (p *Parser) applyIndexConstraint(table *core.Table, name string, indexCols []core.IndexColumn, indexType core.IndexType) {
	table.Indexes = append(table.Indexes, &core.Index{
		Name:    name,
		Columns: indexCols,
		Unique:  false,
		Type:    indexType,
	})
}

func (p *Parser) applyCheckConstraint(table *core.Table, constraint *ast.Constraint, columns []string) {
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

//nolint:revive // Character validation requires checking multiple ranges
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
