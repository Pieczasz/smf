package mysql

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"smf/internal/core"
	"smf/internal/diff"
)

func (g *Generator) tableOptions(t *core.Table) string {
	var parts []string
	o := t.Options

	parts = g.addBasicTableOptions(parts, o)
	parts = g.addStorageTableOptions(parts, o)
	parts = g.addSecurityOptions(parts, o, t.Comment)

	if len(parts) == 0 {
		return ""
	}
	return " " + strings.Join(parts, " ")
}

func (g *Generator) addBasicTableOptions(parts []string, o core.TableOptions) []string {
	if engine := strings.TrimSpace(o.Engine); engine != "" {
		parts = append(parts, "ENGINE="+engine)
	}
	if charset := strings.TrimSpace(o.Charset); charset != "" {
		parts = append(parts, "DEFAULT CHARSET="+charset)
	}
	if collate := strings.TrimSpace(o.Collate); collate != "" {
		parts = append(parts, "COLLATE="+collate)
	}
	if o.AutoIncrement != 0 {
		parts = append(parts, "AUTO_INCREMENT="+strconv.FormatUint(o.AutoIncrement, 10))
	}
	if rowFormat := strings.TrimSpace(o.RowFormat); rowFormat != "" {
		parts = append(parts, "ROW_FORMAT="+rowFormat)
	}
	return parts
}

func (g *Generator) addStorageTableOptions(parts []string, o core.TableOptions) []string {
	if o.AvgRowLength != 0 {
		parts = append(parts, "AVG_ROW_LENGTH="+strconv.FormatUint(o.AvgRowLength, 10))
	}
	if o.KeyBlockSize != 0 {
		parts = append(parts, "KEY_BLOCK_SIZE="+strconv.FormatUint(o.KeyBlockSize, 10))
	}
	if o.MaxRows != 0 {
		parts = append(parts, "MAX_ROWS="+strconv.FormatUint(o.MaxRows, 10))
	}
	if o.MinRows != 0 {
		parts = append(parts, "MIN_ROWS="+strconv.FormatUint(o.MinRows, 10))
	}
	return parts
}

func (g *Generator) addSecurityOptions(parts []string, o core.TableOptions, comment string) []string {
	if compression := strings.TrimSpace(o.Compression); compression != "" {
		parts = append(parts, "COMPRESSION="+g.QuoteString(compression))
	}
	if encryption := strings.TrimSpace(o.Encryption); encryption != "" {
		parts = append(parts, "ENCRYPTION="+g.QuoteString(encryption))
	}
	if tablespace := strings.TrimSpace(o.Tablespace); tablespace != "" {
		parts = append(parts, "TABLESPACE "+g.QuoteIdentifier(tablespace))
	}
	if cmt := strings.TrimSpace(comment); cmt != "" {
		parts = append(parts, "COMMENT="+g.QuoteString(cmt))
	}
	return parts
}

func (g *Generator) columnDefinition(c *core.Column) string {
	var parts []string

	// TODO: dialect recognistion
	effectiveType := c.EffectiveType("mysql")
	parts = append(parts, g.QuoteIdentifier(c.Name), sanitizeMySQLTypeRaw(effectiveType))
	parts = g.addGeneratedColumn(parts, c)
	parts = g.addNullability(parts, c)
	parts = g.addAutoAttributes(parts, c)
	parts = g.addCharsetCollation(parts, c)
	parts = g.addDefaultAndUpdate(parts, c)
	parts = g.addColumnFormat(parts, c)

	return strings.Join(parts, " ")
}

func (g *Generator) addGeneratedColumn(parts []string, c *core.Column) []string {
	if c.IsGenerated {
		expr := strings.TrimSpace(c.GenerationExpression)
		if expr != "" {
			storage := strings.ToUpper(strings.TrimSpace(string(c.GenerationStorage)))
			if storage == "" {
				storage = "VIRTUAL"
			}
			parts = append(parts, fmt.Sprintf("GENERATED ALWAYS AS (%s) %s", expr, storage))
		}
	}
	return parts
}

func (g *Generator) addNullability(parts []string, c *core.Column) []string {
	if c.Nullable {
		parts = append(parts, "NULL")
	} else {
		parts = append(parts, "NOT NULL")
	}
	return parts
}

func (g *Generator) addAutoAttributes(parts []string, c *core.Column) []string {
	if c.AutoIncrement {
		parts = append(parts, "AUTO_INCREMENT")
	}
	if c.AutoRandom > 0 {
		parts = append(parts, fmt.Sprintf("AUTO_RANDOM(%d)", c.AutoRandom))
	}
	return parts
}

func (g *Generator) addCharsetCollation(parts []string, c *core.Column) []string {
	if supportsCharsetCollation(c.TypeRaw) {
		if cs := strings.TrimSpace(c.Charset); cs != "" {
			parts = append(parts, "CHARACTER SET", cs)
		}
		if coll := strings.TrimSpace(c.Collate); coll != "" {
			parts = append(parts, "COLLATE", coll)
		}
	}
	return parts
}

func (g *Generator) addDefaultAndUpdate(parts []string, c *core.Column) []string {
	if c.DefaultValue != nil {
		parts = append(parts, "DEFAULT", g.formatValue(*c.DefaultValue))
	}
	if c.OnUpdate != nil {
		parts = append(parts, "ON UPDATE", g.formatValue(*c.OnUpdate))
	}
	return parts
}

func (g *Generator) addColumnFormat(parts []string, c *core.Column) []string {
	if colFmt := strings.TrimSpace(c.ColumnFormat); colFmt != "" {
		parts = append(parts, "COLUMN_FORMAT", strings.ToUpper(colFmt))
	}
	if stor := strings.TrimSpace(c.Storage); stor != "" {
		parts = append(parts, "STORAGE", strings.ToUpper(stor))
	}
	if comment := strings.TrimSpace(c.Comment); comment != "" {
		parts = append(parts, "COMMENT", g.QuoteString(comment))
	}
	return parts
}

func (g *Generator) indexDefinitionInline(idx *core.Index) string {
	cols := g.formatIndexColumns(idx.Columns)
	name := strings.TrimSpace(idx.Name)
	if name == "" {
		return ""
	}

	typ := strings.ToUpper(strings.TrimSpace(string(idx.Type)))
	switch {
	case idx.Unique:
		return fmt.Sprintf("UNIQUE KEY %s %s", g.QuoteIdentifier(name), cols)
	case typ == "FULLTEXT":
		return fmt.Sprintf("FULLTEXT KEY %s %s", g.QuoteIdentifier(name), cols)
	case typ == "SPATIAL":
		return fmt.Sprintf("SPATIAL KEY %s %s", g.QuoteIdentifier(name), cols)
	default:
		return fmt.Sprintf("KEY %s %s", g.QuoteIdentifier(name), cols)
	}
}

func (g *Generator) createIndex(table string, idx *core.Index) string {
	if idx == nil {
		return ""
	}

	name := strings.TrimSpace(idx.Name)
	if name == "" {
		return ""
	}

	cols := g.formatIndexColumns(idx.Columns)
	typ := strings.ToUpper(strings.TrimSpace(string(idx.Type)))

	switch {
	case idx.Unique:
		return fmt.Sprintf("CREATE UNIQUE INDEX %s ON %s %s;", g.QuoteIdentifier(name), table, cols)
	case typ == "FULLTEXT":
		return fmt.Sprintf("CREATE FULLTEXT INDEX %s ON %s %s;", g.QuoteIdentifier(name), table, cols)
	case typ == "SPATIAL":
		return fmt.Sprintf("CREATE SPATIAL INDEX %s ON %s %s;", g.QuoteIdentifier(name), table, cols)
	default:
		return fmt.Sprintf("CREATE INDEX %s ON %s %s;", g.QuoteIdentifier(name), table, cols)
	}
}

func (g *Generator) constraintDefinition(c *core.Constraint) string {
	cols := g.formatColumns(c.Columns)

	switch c.Type {
	case core.ConstraintPrimaryKey:
		return fmt.Sprintf("PRIMARY KEY %s", cols)
	case core.ConstraintUnique:
		if name := strings.TrimSpace(c.Name); name != "" {
			return fmt.Sprintf("CONSTRAINT %s UNIQUE KEY %s", g.QuoteIdentifier(name), cols)
		}
		return fmt.Sprintf("UNIQUE KEY %s", cols)
	case core.ConstraintCheck:
		expr := strings.TrimSpace(c.CheckExpression)
		if expr == "" {
			return ""
		}
		if name := strings.TrimSpace(c.Name); name != "" {
			return fmt.Sprintf("CONSTRAINT %s CHECK (%s)", g.QuoteIdentifier(name), expr)
		}
		return fmt.Sprintf("CHECK (%s)", expr)
	default:
		return ""
	}
}

func (g *Generator) addConstraint(table string, c *core.Constraint) string {
	if c == nil {
		return ""
	}

	cols := g.formatColumns(c.Columns)

	switch c.Type {
	case core.ConstraintPrimaryKey:
		return g.addPrimaryKeyConstraint(table, cols)
	case core.ConstraintUnique:
		return g.addUniqueConstraint(table, c.Name, cols)
	case core.ConstraintForeignKey:
		return g.addForeignKeyConstraint(table, c, cols)
	case core.ConstraintCheck:
		return g.addCheckConstraint(table, c)
	default:
		return ""
	}
}

func (g *Generator) addPrimaryKeyConstraint(table, cols string) string {
	return fmt.Sprintf("ALTER TABLE %s ADD PRIMARY KEY %s;", table, cols)
}

func (g *Generator) addUniqueConstraint(table, name, cols string) string {
	if name := strings.TrimSpace(name); name != "" {
		return fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s UNIQUE %s;", table, g.QuoteIdentifier(name), cols)
	}
	return fmt.Sprintf("ALTER TABLE %s ADD UNIQUE %s;", table, cols)
}

func (g *Generator) addForeignKeyConstraint(table string, c *core.Constraint, cols string) string {
	if len(c.Columns) == 0 || strings.TrimSpace(c.ReferencedTable) == "" {
		return ""
	}
	var sb strings.Builder
	sb.Grow(128)
	sb.WriteString("ALTER TABLE ")
	sb.WriteString(table)
	sb.WriteString(" ADD ")
	if name := strings.TrimSpace(c.Name); name != "" {
		sb.WriteString("CONSTRAINT ")
		sb.WriteString(g.QuoteIdentifier(name))
		sb.WriteString(" ")
	}
	sb.WriteString("FOREIGN KEY ")
	sb.WriteString(cols)
	sb.WriteString(" REFERENCES ")
	sb.WriteString(g.QuoteIdentifier(c.ReferencedTable))
	sb.WriteString(" ")
	sb.WriteString(g.formatColumns(c.ReferencedColumns))
	if del := strings.TrimSpace(string(c.OnDelete)); del != "" {
		sb.WriteString(" ON DELETE ")
		sb.WriteString(del)
	}
	if upd := strings.TrimSpace(string(c.OnUpdate)); upd != "" {
		sb.WriteString(" ON UPDATE ")
		sb.WriteString(upd)
	}
	sb.WriteString(";")
	return sb.String()
}

func (g *Generator) addCheckConstraint(table string, c *core.Constraint) string {
	expr := strings.TrimSpace(c.CheckExpression)
	if expr == "" {
		return ""
	}
	if name := strings.TrimSpace(c.Name); name != "" {
		return fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s CHECK (%s);", table, g.QuoteIdentifier(name), expr)
	}
	return fmt.Sprintf("ALTER TABLE %s ADD CHECK (%s);", table, expr)
}

func (g *Generator) dropConstraint(table string, c *core.Constraint) string {
	if c == nil {
		return ""
	}

	switch c.Type {
	case core.ConstraintPrimaryKey:
		return g.dropPrimaryKey(table)
	case core.ConstraintForeignKey:
		return g.dropForeignKey(table, c)
	case core.ConstraintUnique:
		return g.dropUnique(table, c)
	case core.ConstraintCheck:
		return g.dropCheck(table, c)
	default:
		return ""
	}
}

func (g *Generator) dropPrimaryKey(table string) string {
	return fmt.Sprintf("ALTER TABLE %s DROP PRIMARY KEY;", table)
}

func (g *Generator) dropForeignKey(table string, c *core.Constraint) string {
	if name := strings.TrimSpace(c.Name); name != "" {
		return fmt.Sprintf("ALTER TABLE %s DROP FOREIGN KEY %s;", table, g.QuoteIdentifier(name))
	}
	cols := strings.Join(c.Columns, ",")
	if cols != "" {
		cols = " (" + cols + ")"
	}
	return fmt.Sprintf("-- cannot drop unnamed FOREIGN KEY%s on %s", cols, table)
}

func (g *Generator) dropUnique(table string, c *core.Constraint) string {
	if name := strings.TrimSpace(c.Name); name != "" {
		return fmt.Sprintf("ALTER TABLE %s DROP INDEX %s;", table, g.QuoteIdentifier(name))
	}
	cols := strings.Join(c.Columns, ",")
	if cols != "" {
		cols = " (" + cols + ")"
	}
	return fmt.Sprintf("-- cannot drop unnamed UNIQUE%s on %s", cols, table)
}

func (g *Generator) dropCheck(table string, c *core.Constraint) string {
	if name := strings.TrimSpace(c.Name); name != "" {
		return fmt.Sprintf("ALTER TABLE %s DROP CHECK %s;", table, g.QuoteIdentifier(name))
	}
	return fmt.Sprintf("-- cannot drop unnamed CHECK on %s", table)
}

// reValidOptionName matches only safe MySQL table option names (alphanumeric and underscores).
var reValidOptionName = regexp.MustCompile(`^[A-Z][A-Z0-9_]*$`)

func (g *Generator) alterOption(table string, opt *diff.TableOptionChange) string {
	name := strings.ToUpper(strings.TrimSpace(opt.Name))
	value := strings.TrimSpace(opt.New)

	if value == "" {
		return ""
	}

	switch name {
	case "ENGINE":
		return fmt.Sprintf("ALTER TABLE %s ENGINE=%s;", table, value)
	case "AUTO_INCREMENT":
		return fmt.Sprintf("ALTER TABLE %s AUTO_INCREMENT=%s;", table, value)
	case "CHARSET":
		return fmt.Sprintf("ALTER TABLE %s DEFAULT CHARSET=%s;", table, value)
	case "COLLATE":
		return fmt.Sprintf("ALTER TABLE %s COLLATE=%s;", table, value)
	case "COMMENT":
		return fmt.Sprintf("ALTER TABLE %s COMMENT=%s;", table, g.QuoteString(value))
	case "ROW_FORMAT":
		return fmt.Sprintf("ALTER TABLE %s ROW_FORMAT=%s;", table, value)
	default:
		return g.alterGenericOption(table, name, value)
	}
}

func (g *Generator) alterGenericOption(table, name, value string) string {
	if !reValidOptionName.MatchString(name) {
		// TODO: consider adding a warning instead of silently returning empty string
		return ""
	}
	if _, err := strconv.ParseFloat(value, 64); err == nil {
		return fmt.Sprintf("ALTER TABLE %s %s=%s;", table, name, value)
	}
	return fmt.Sprintf("ALTER TABLE %s %s=%s;", table, name, g.QuoteString(value))
}

var reBaseType = regexp.MustCompile(`(?i)^\s*([a-z0-9_]+)\b`)

func supportsCharsetCollation(typeRaw string) bool {
	m := reBaseType.FindStringSubmatch(typeRaw)
	if len(m) < 2 {
		return false
	}
	base := strings.ToLower(strings.TrimSpace(m[1]))
	switch base {
	case "char", "varchar", "tinytext", "text", "mediumtext", "longtext", "enum", "set":
		return true
	default:
		return false
	}
}

func sanitizeMySQLTypeRaw(typeRaw string) string {
	tr := strings.TrimSpace(typeRaw)
	if tr == "" {
		return tr
	}

	m := reBaseType.FindStringSubmatch(tr)
	if len(m) < 2 {
		return tr
	}
	base := strings.ToLower(strings.TrimSpace(m[1]))

	if base == "varbinary" || base == "binary" {
		tokens := strings.Fields(tr)
		if len(tokens) >= 2 && strings.EqualFold(tokens[len(tokens)-1], "BINARY") {
			return strings.Join(tokens[:len(tokens)-1], " ")
		}
	}

	return tr
}
