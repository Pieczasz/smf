package dialect

import (
	"fmt"
	"regexp"
	"schemift/core"
	"schemift/parser/mysql"
	"strconv"
	"strings"
)

type MySQLDialect struct {
	generator *MySQLGenerator
	parser    *mysql.Parser
}

func NewMySQLDialect() *MySQLDialect {
	return &MySQLDialect{
		generator: NewMySQLGenerator(),
		parser:    mysql.NewParser(),
	}
}

func (d *MySQLDialect) Name() core.Dialect {
	return core.DialectMySQL
}

func (d *MySQLDialect) Generator() Generator {
	return d.generator
}

func (d *MySQLDialect) Parser() Parser {
	return d.parser
}

type MySQLGenerator struct{}

func NewMySQLGenerator() *MySQLGenerator {
	return &MySQLGenerator{}
}

func (g *MySQLGenerator) GenerateMigration(diff *core.SchemaDiff) *core.Migration {
	m := &core.Migration{}
	if diff == nil {
		m.Notes = append(m.Notes, "No diff provided; nothing to migrate.")
		return m
	}

	analyzer := core.NewBreakingChangeAnalyzer()
	breakingChanges := analyzer.Analyze(diff)
	for _, bc := range breakingChanges {
		switch bc.Severity {
		case core.SeverityCritical, core.SeverityBreaking:
			m.Breaking = append(m.Breaking, fmt.Sprintf("[%s] %s.%s: %s", bc.Severity, bc.Table, bc.Object, bc.Description))
		case core.SeverityWarning:
			m.Notes = append(m.Notes, fmt.Sprintf("[WARNING] %s.%s: %s", bc.Table, bc.Object, bc.Description))
		case core.SeverityInfo:
		}

		m.Notes = append(m.Notes, migrationRecommendations(bc)...)
	}

	var pendingFKs []string

	for _, t := range diff.AddedTables {
		if t == nil {
			continue
		}
		create, fks := g.GenerateCreateTable(t)
		m.Statements = append(m.Statements, create)
		pendingFKs = append(pendingFKs, fks...)
	}

	for _, td := range diff.ModifiedTables {
		if td == nil {
			continue
		}
		stmts, fkAdds := g.generateAlterTable(td)
		m.Statements = append(m.Statements, stmts...)
		pendingFKs = append(pendingFKs, fkAdds...)
	}

	if len(pendingFKs) > 0 {
		m.Notes = append(m.Notes, "Foreign keys added after table creation to avoid dependency issues.")
		m.Statements = append(m.Statements, pendingFKs...)
	}

	for _, t := range diff.RemovedTables {
		if t == nil {
			continue
		}
		m.Statements = append(m.Statements, g.GenerateDropTable(t))
	}

	if hasPotentiallyLockingStatements(m.Statements) {
		m.Notes = append(m.Notes, "Lock-time warning: ALTER TABLE / index changes may lock or rebuild tables; for large tables consider online schema change tools and off-peak execution.")
	}

	rollback := g.rollbackSuggestions(diff)
	if len(rollback) > 0 {
		m.Notes = append(m.Notes, "Suggested rollback (best-effort; review carefully):")
		for _, stmt := range rollback {
			m.Notes = append(m.Notes, "ROLLBACK: "+stmt)
		}
	}

	m.Notes = dedupeStable(m.Notes)
	m.Breaking = dedupeStable(m.Breaking)

	return m
}

func hasPotentiallyLockingStatements(stmts []string) bool {
	for _, s := range stmts {
		u := strings.ToUpper(strings.TrimSpace(s))
		if strings.HasPrefix(u, "ALTER TABLE") || strings.HasPrefix(u, "CREATE INDEX") || strings.HasPrefix(u, "DROP INDEX") {
			return true
		}
	}
	return false
}

func migrationRecommendations(bc core.BreakingChange) []string {
	// Keep these short: they end up as "-- - ..." comments.
	msg := strings.ToLower(bc.Description)
	var out []string

	switch {
	case strings.Contains(msg, "column rename detected"):
		out = append(out, fmt.Sprintf("Data migration tip: use an explicit rename (e.g. CHANGE COLUMN) for %s.%s to preserve data.", bc.Table, bc.Object))
	case strings.Contains(msg, "becomes not null"):
		out = append(out, fmt.Sprintf("Data migration tip: backfill %s.%s (UPDATE NULLs) before enforcing NOT NULL.", bc.Table, bc.Object))
	case strings.Contains(msg, "adding not null column without default"):
		out = append(out, fmt.Sprintf("Data migration tip: add %s.%s as NULL first, backfill, then ALTER to NOT NULL.", bc.Table, bc.Object))
	case strings.Contains(msg, "type changes"):
		out = append(out, fmt.Sprintf("Data migration tip: validate cast/backfill for %s.%s before applying the type change.", bc.Table, bc.Object))
	case strings.Contains(msg, "length shrinks"):
		out = append(out, fmt.Sprintf("Data migration tip: check max length in %s.%s before shrinking (e.g. MAX(CHAR_LENGTH(col))).", bc.Table, bc.Object))
	case strings.Contains(msg, "table will be dropped"):
		out = append(out, fmt.Sprintf("Safety tip: take a backup or copy data out of %s before DROP TABLE.", bc.Table))
	case strings.Contains(msg, "column will be dropped"):
		out = append(out, fmt.Sprintf("Safety tip: take a backup or copy data out of %s.%s before DROP COLUMN.", bc.Table, bc.Object))
	}

	return out
}

func (g *MySQLGenerator) rollbackSuggestions(diff *core.SchemaDiff) []string {
	if diff == nil {
		return nil
	}
	var out []string

	for _, t := range diff.AddedTables {
		if t == nil {
			continue
		}
		out = append(out, g.GenerateDropTable(t))
	}

	for _, t := range diff.RemovedTables {
		if t == nil {
			continue
		}
		out = append(out, fmt.Sprintf("-- cannot auto-rollback DROP TABLE %s (restore from backup)", g.QuoteIdentifier(t.Name)))
	}

	for _, td := range diff.ModifiedTables {
		if td == nil {
			continue
		}
		table := g.QuoteIdentifier(td.Name)

		for _, ac := range td.AddedColumns {
			if ac == nil {
				continue
			}
			out = append(out, fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s;", table, g.QuoteIdentifier(ac.Name)))
		}
		for _, rc := range td.RemovedColumns {
			if rc == nil {
				continue
			}
			out = append(out, fmt.Sprintf("-- data not restored; best-effort column re-add\nALTER TABLE %s ADD COLUMN %s;", table, g.columnDefinition(rc)))
		}
		for _, mc := range td.ModifiedColumns {
			if mc == nil || mc.Old == nil {
				continue
			}
			out = append(out, fmt.Sprintf("ALTER TABLE %s MODIFY COLUMN %s;", table, g.columnDefinition(mc.Old)))
		}

		for _, ac := range td.AddedConstraints {
			if drop := g.dropConstraint(table, ac); drop != "" {
				out = append(out, drop)
			}
		}
		for _, rc := range td.RemovedConstraints {
			if add := g.addConstraint(table, rc); add != "" {
				out = append(out, add)
			}
		}
		for _, mc := range td.ModifiedConstraints {
			if mc == nil {
				continue
			}
			if drop := g.dropConstraint(table, mc.New); drop != "" {
				out = append(out, drop)
			}
			if add := g.addConstraint(table, mc.Old); add != "" {
				out = append(out, add)
			}
		}

		for _, ai := range td.AddedIndexes {
			if ai == nil || strings.TrimSpace(ai.Name) == "" {
				continue
			}
			out = append(out, fmt.Sprintf("DROP INDEX %s ON %s;", g.QuoteIdentifier(ai.Name), table))
		}
		for _, ri := range td.RemovedIndexes {
			if ri == nil {
				continue
			}
			out = append(out, g.createIndex(table, ri))
		}
		for _, mi := range td.ModifiedIndexes {
			if mi == nil {
				continue
			}
			if mi.New != nil && strings.TrimSpace(mi.New.Name) != "" {
				out = append(out, fmt.Sprintf("DROP INDEX %s ON %s;", g.QuoteIdentifier(mi.New.Name), table))
			}
			out = append(out, g.createIndex(table, mi.Old))
		}

		for _, mo := range td.ModifiedOptions {
			if mo == nil {
				continue
			}
			// Reverse the change.
			if stmt := g.alterOption(table, &core.TableOptionChange{Name: mo.Name, New: mo.Old}); stmt != "" {
				out = append(out, stmt)
			}
		}
	}

	var cleaned []string
	for _, s := range out {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		cleaned = append(cleaned, s)
	}
	return cleaned
}

func (g *MySQLGenerator) GenerateCreateTable(t *core.Table) (string, []string) {
	name := g.QuoteIdentifier(t.Name)

	var lines []string
	for _, c := range t.Columns {
		if c == nil {
			continue
		}
		lines = append(lines, "  "+g.columnDefinition(c))
	}

	var fks []*core.Constraint
	for _, c := range t.Constraints {
		if c == nil {
			continue
		}
		if c.Type == core.ConstraintForeignKey {
			fks = append(fks, c)
			continue
		}
		if line := g.constraintDefinition(c); line != "" {
			lines = append(lines, "  "+line)
		}
	}

	for _, idx := range t.Indexes {
		if idx == nil {
			continue
		}
		if line := g.indexDefinitionInline(idx); line != "" {
			lines = append(lines, "  "+line)
		}
	}

	options := g.tableOptions(t)
	create := fmt.Sprintf("CREATE TABLE %s (\n%s\n)%s;", name, strings.Join(lines, ",\n"), options)

	var fkStmts []string
	for _, fk := range fks {
		stmt := g.addConstraint(name, fk)
		if stmt != "" {
			fkStmts = append(fkStmts, stmt)
		}
	}

	return create, fkStmts
}

func (g *MySQLGenerator) GenerateDropTable(t *core.Table) string {
	return fmt.Sprintf("DROP TABLE %s;", g.QuoteIdentifier(t.Name))
}

func (g *MySQLGenerator) GenerateAlterTable(td *core.TableDiff) []string {
	stmts, fkAdds := g.generateAlterTable(td)
	return append(stmts, fkAdds...)
}

func (g *MySQLGenerator) generateAlterTable(td *core.TableDiff) ([]string, []string) {
	table := g.QuoteIdentifier(td.Name)
	var stmts []string
	var fkAdds []string

	for _, ch := range td.ModifiedConstraints {
		if ch == nil {
			continue
		}
		if drop := g.dropConstraint(table, ch.Old); drop != "" {
			stmts = append(stmts, drop)
		}
	}
	for _, rc := range td.RemovedConstraints {
		if drop := g.dropConstraint(table, rc); drop != "" {
			stmts = append(stmts, drop)
		}
	}

	for _, mi := range td.ModifiedIndexes {
		if mi == nil || mi.Old == nil || strings.TrimSpace(mi.Old.Name) == "" {
			continue
		}
		stmts = append(stmts, fmt.Sprintf("DROP INDEX %s ON %s;", g.QuoteIdentifier(mi.Old.Name), table))
	}
	for _, ri := range td.RemovedIndexes {
		if ri == nil || strings.TrimSpace(ri.Name) == "" {
			continue
		}
		stmts = append(stmts, fmt.Sprintf("DROP INDEX %s ON %s;", g.QuoteIdentifier(ri.Name), table))
	}

	for _, c := range td.AddedColumns {
		if c == nil {
			continue
		}
		stmts = append(stmts, fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s;", table, g.columnDefinition(c)))
	}
	for _, ch := range td.ModifiedColumns {
		if ch == nil || ch.New == nil {
			continue
		}
		stmts = append(stmts, fmt.Sprintf("ALTER TABLE %s MODIFY COLUMN %s;", table, g.columnDefinition(ch.New)))
	}
	for _, c := range td.RemovedColumns {
		if c == nil {
			continue
		}
		stmts = append(stmts, fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s;", table, g.QuoteIdentifier(c.Name)))
	}

	for _, mo := range td.ModifiedOptions {
		if mo == nil {
			continue
		}
		if stmt := g.alterOption(table, mo); stmt != "" {
			stmts = append(stmts, stmt)
		}
	}

	for _, mi := range td.ModifiedIndexes {
		if mi == nil {
			continue
		}
		stmts = append(stmts, g.createIndex(table, mi.New))
	}
	for _, ai := range td.AddedIndexes {
		if ai == nil {
			continue
		}
		stmts = append(stmts, g.createIndex(table, ai))
	}

	for _, ch := range td.ModifiedConstraints {
		if ch == nil {
			continue
		}
		if ch.New != nil && ch.New.Type == core.ConstraintForeignKey {
			if add := g.addConstraint(table, ch.New); add != "" {
				fkAdds = append(fkAdds, add)
			}
			continue
		}
		if add := g.addConstraint(table, ch.New); add != "" {
			stmts = append(stmts, add)
		}
	}
	for _, ac := range td.AddedConstraints {
		if ac == nil {
			continue
		}
		if ac.Type == core.ConstraintForeignKey {
			if add := g.addConstraint(table, ac); add != "" {
				fkAdds = append(fkAdds, add)
			}
			continue
		}
		if add := g.addConstraint(table, ac); add != "" {
			stmts = append(stmts, add)
		}
	}

	return stmts, fkAdds
}

func (g *MySQLGenerator) QuoteIdentifier(name string) string {
	name = strings.TrimSpace(name)
	name = strings.ReplaceAll(name, "`", "``")
	return "`" + name + "`"
}

func (g *MySQLGenerator) QuoteString(value string) string {
	value = strings.ReplaceAll(value, "\\", "\\\\")
	value = strings.ReplaceAll(value, "'", "\\'")
	return "'" + value + "'"
}

func (g *MySQLGenerator) columnDefinition(c *core.Column) string {
	var parts []string
	parts = append(parts, g.QuoteIdentifier(c.Name), sanitizeMySQLTypeRaw(strings.TrimSpace(c.TypeRaw)))

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

	if c.Nullable {
		parts = append(parts, "NULL")
	} else {
		parts = append(parts, "NOT NULL")
	}

	if c.AutoIncrement {
		parts = append(parts, "AUTO_INCREMENT")
	}

	if c.AutoRandom > 0 {
		parts = append(parts, fmt.Sprintf("AUTO_RANDOM(%d)", c.AutoRandom))
	}

	if supportsCharsetCollation(c.TypeRaw) {
		if cs := strings.TrimSpace(c.Charset); cs != "" {
			parts = append(parts, "CHARACTER SET", cs)
		}
		if coll := strings.TrimSpace(c.Collate); coll != "" {
			parts = append(parts, "COLLATE", coll)
		}
	}

	if c.DefaultValue != nil {
		parts = append(parts, "DEFAULT", g.formatValue(*c.DefaultValue))
	}

	if c.OnUpdate != nil {
		parts = append(parts, "ON UPDATE", g.formatValue(*c.OnUpdate))
	}

	if colFmt := strings.TrimSpace(c.ColumnFormat); colFmt != "" {
		parts = append(parts, "COLUMN_FORMAT", strings.ToUpper(colFmt))
	}

	if stor := strings.TrimSpace(c.Storage); stor != "" {
		parts = append(parts, "STORAGE", strings.ToUpper(stor))
	}

	if comment := strings.TrimSpace(c.Comment); comment != "" {
		parts = append(parts, "COMMENT", g.QuoteString(comment))
	}

	return strings.Join(parts, " ")
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
		toks := strings.Fields(tr)
		if len(toks) >= 2 && strings.EqualFold(toks[len(toks)-1], "BINARY") {
			return strings.Join(toks[:len(toks)-1], " ")
		}
	}

	return tr
}

func (g *MySQLGenerator) constraintDefinition(c *core.Constraint) string {
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

func (g *MySQLGenerator) indexDefinitionInline(idx *core.Index) string {
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

func (g *MySQLGenerator) tableOptions(t *core.Table) string {
	var parts []string
	o := t.Options

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
	if compression := strings.TrimSpace(o.Compression); compression != "" {
		parts = append(parts, "COMPRESSION="+g.QuoteString(compression))
	}
	if encryption := strings.TrimSpace(o.Encryption); encryption != "" {
		parts = append(parts, "ENCRYPTION="+g.QuoteString(encryption))
	}
	if tablespace := strings.TrimSpace(o.Tablespace); tablespace != "" {
		parts = append(parts, "TABLESPACE "+g.QuoteIdentifier(tablespace))
	}
	if comment := strings.TrimSpace(t.Comment); comment != "" {
		parts = append(parts, "COMMENT="+g.QuoteString(comment))
	}

	if len(parts) == 0 {
		return ""
	}
	return " " + strings.Join(parts, " ")
}

func (g *MySQLGenerator) addConstraint(table string, c *core.Constraint) string {
	if c == nil {
		return ""
	}

	cols := g.formatColumns(c.Columns)

	switch c.Type {
	case core.ConstraintPrimaryKey:
		return fmt.Sprintf("ALTER TABLE %s ADD PRIMARY KEY %s;", table, cols)
	case core.ConstraintUnique:
		if name := strings.TrimSpace(c.Name); name != "" {
			return fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s UNIQUE %s;", table, g.QuoteIdentifier(name), cols)
		}
		return fmt.Sprintf("ALTER TABLE %s ADD UNIQUE %s;", table, cols)
	case core.ConstraintForeignKey:
		if len(c.Columns) == 0 || strings.TrimSpace(c.ReferencedTable) == "" {
			return ""
		}
		var sb strings.Builder
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
	case core.ConstraintCheck:
		expr := strings.TrimSpace(c.CheckExpression)
		if expr == "" {
			return ""
		}
		if name := strings.TrimSpace(c.Name); name != "" {
			return fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s CHECK (%s);", table, g.QuoteIdentifier(name), expr)
		}
		return fmt.Sprintf("ALTER TABLE %s ADD CHECK (%s);", table, expr)
	default:
		return ""
	}
}

func (g *MySQLGenerator) dropConstraint(table string, c *core.Constraint) string {
	if c == nil {
		return ""
	}

	switch c.Type {
	case core.ConstraintPrimaryKey:
		return fmt.Sprintf("ALTER TABLE %s DROP PRIMARY KEY;", table)
	case core.ConstraintForeignKey:
		if name := strings.TrimSpace(c.Name); name != "" {
			return fmt.Sprintf("ALTER TABLE %s DROP FOREIGN KEY %s;", table, g.QuoteIdentifier(name))
		}
		return ""
	case core.ConstraintUnique:
		if name := strings.TrimSpace(c.Name); name != "" {
			return fmt.Sprintf("ALTER TABLE %s DROP INDEX %s;", table, g.QuoteIdentifier(name))
		}
		return ""
	case core.ConstraintCheck:
		if name := strings.TrimSpace(c.Name); name != "" {
			return fmt.Sprintf("ALTER TABLE %s DROP CHECK %s;", table, g.QuoteIdentifier(name))
		}
		return ""
	default:
		return ""
	}
}

func (g *MySQLGenerator) createIndex(table string, idx *core.Index) string {
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

func (g *MySQLGenerator) alterOption(table string, opt *core.TableOptionChange) string {
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
		if looksNumeric(value) {
			return fmt.Sprintf("ALTER TABLE %s %s=%s;", table, name, value)
		}
		return fmt.Sprintf("ALTER TABLE %s %s=%s;", table, name, g.QuoteString(value))
	}
}

func (g *MySQLGenerator) formatColumns(cols []string) string {
	var quoted []string
	for _, c := range cols {
		c = strings.TrimSpace(c)
		if c == "" {
			continue
		}
		quoted = append(quoted, g.QuoteIdentifier(c))
	}
	return "(" + strings.Join(quoted, ", ") + ")"
}

func (g *MySQLGenerator) formatIndexColumns(cols []core.IndexColumn) string {
	var quoted []string
	for _, c := range cols {
		name := strings.TrimSpace(c.Name)
		if name == "" {
			continue
		}
		qname := g.QuoteIdentifier(name)
		if c.Length > 0 {
			qname = fmt.Sprintf("%s(%d)", qname, c.Length)
		}
		quoted = append(quoted, qname)
	}
	return "(" + strings.Join(quoted, ", ") + ")"
}

func (g *MySQLGenerator) formatValue(v string) string {
	v = strings.TrimSpace(v)
	if v == "" {
		return "''"
	}

	upper := strings.ToUpper(v)
	keywords := []string{"NULL", "CURRENT_TIMESTAMP", "CURRENT_DATE", "CURRENT_TIME", "NOW()", "TRUE", "FALSE"}
	for _, kw := range keywords {
		if upper == kw {
			return upper
		}
	}

	if looksNumeric(v) {
		return v
	}

	if strings.ContainsAny(v, "()") {
		return v
	}

	return g.QuoteString(v)
}

func looksNumeric(s string) bool {
	_, err := strconv.ParseFloat(s, 64)
	return err == nil
}

func dedupeStable(items []string) []string {
	seen := make(map[string]struct{}, len(items))
	var out []string
	for _, item := range items {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		out = append(out, item)
	}
	return out
}
