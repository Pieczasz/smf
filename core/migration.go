package core

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

type Migration struct {
	Statements []string
	Breaking   []string
	Notes      []string
	Unresolved []string
}

func Migrate(schemaDiff *SchemaDiff) *Migration {
	m := &Migration{}
	if schemaDiff == nil {
		m.Notes = append(m.Notes, "No diff provided; nothing to migrate.")
		return m
	}

	// CREATE new tables first (without foreign keys, we add FKs later after tables exist).
	var pendingFKAdds []string
	for _, t := range schemaDiff.AddedTables {
		if t == nil {
			continue
		}
		create, fks, notes := buildCreateTableMySQL(t)
		m.Statements = append(m.Statements, create)
		pendingFKAdds = append(pendingFKAdds, fks...)
		m.Notes = append(m.Notes, notes...)
	}

	// ALTER existing tables.
	for _, td := range schemaDiff.ModifiedTables {
		if td == nil {
			continue
		}
		migrateTableDiffMySQL(m, td)
	}

	// Add foreign keys for new tables after everything exists.
	if len(pendingFKAdds) > 0 {
		m.Notes = append(m.Notes, "Foreign keys for newly created tables are applied after CREATE TABLE to avoid dependency ordering issues.")
		m.Statements = append(m.Statements, pendingFKAdds...)
	}

	// DROP removed tables last.
	for _, t := range schemaDiff.RemovedTables {
		if t == nil {
			continue
		}
		m.Breaking = append(m.Breaking, fmt.Sprintf("Table %s will be dropped (data loss).", quoteIdent(t.Name)))
		m.Statements = append(m.Statements, fmt.Sprintf("DROP TABLE %s;", quoteIdent(t.Name)))
		m.Notes = append(m.Notes, fmt.Sprintf("If other tables have foreign keys referencing %s, you must drop those FKs first.", quoteIdent(t.Name)))
	}

	// De-dupe notes for cleaner output.
	m.Notes = dedupeStable(m.Notes)
	m.Breaking = dedupeStable(m.Breaking)
	m.Unresolved = dedupeStable(m.Unresolved)

	return m
}

func (m *Migration) String() string {
	var sb strings.Builder
	sb.WriteString("-- schemift migration\n")
	sb.WriteString("-- Generated SQL is MySQL. Review before running in production.\n")

	if len(m.Breaking) > 0 {
		sb.WriteString("\n-- BREAKING CHANGES (manual review required)\n")
		for _, b := range m.Breaking {
			sb.WriteString("-- - " + b + "\n")
		}
	}

	if len(m.Unresolved) > 0 {
		sb.WriteString("\n-- UNRESOLVED (cannot auto-generate safely)\n")
		for _, u := range m.Unresolved {
			sb.WriteString("-- - " + u + "\n")
		}
	}

	if len(m.Notes) > 0 {
		sb.WriteString("\n-- NOTES\n")
		for _, n := range m.Notes {
			sb.WriteString("-- - " + n + "\n")
		}
	}

	if len(m.Statements) == 0 {
		sb.WriteString("\n-- No SQL statements generated.\n")
		return sb.String()
	}

	sb.WriteString("\n-- SQL\n")
	for _, stmt := range m.Statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		sb.WriteString(stmt)
		if !strings.HasSuffix(stmt, ";") {
			sb.WriteString(";")
		}
		sb.WriteString("\n")
	}

	return sb.String()
}

func (m *Migration) SaveToFile(path string) error {
	return os.WriteFile(path, []byte(m.String()), 0644)
}

func migrateTableDiffMySQL(m *Migration, td *TableDiff) {
	table := quoteIdent(td.Name)

	// Drop constraints/indexes that are removed/modified first (so column drops/modifies are less likely to fail).
	for _, ch := range td.ModifiedConstraints {
		if ch == nil {
			continue
		}
		m.Notes = append(m.Notes, fmt.Sprintf("Constraint %s on %s will be recreated (may fail if existing data violates the new rule).", constraintDisplayName(ch.New), table))
		drop, ok := dropConstraintMySQL(table, ch.Old)
		if !ok {
			m.Unresolved = append(m.Unresolved, fmt.Sprintf("Cannot drop modified constraint %s on %s automatically (missing name/type).", constraintDisplayName(ch.Old), table))
		} else {
			m.Statements = append(m.Statements, drop)
		}
		add, addNotes := addConstraintMySQL(table, ch.New)
		if add == "" {
			m.Unresolved = append(m.Unresolved, fmt.Sprintf("Cannot add modified constraint %s on %s automatically.", constraintDisplayName(ch.New), table))
		} else {
			m.Statements = append(m.Statements, add)
		}
		m.Notes = append(m.Notes, addNotes...)
	}

	for _, rc := range td.RemovedConstraints {
		if rc == nil {
			continue
		}
		m.Breaking = append(m.Breaking, fmt.Sprintf("Constraint %s will be removed from %s.", constraintDisplayName(rc), table))
		drop, ok := dropConstraintMySQL(table, rc)
		if !ok {
			m.Unresolved = append(m.Unresolved, fmt.Sprintf("Cannot drop constraint %s on %s automatically (missing name/type).", constraintDisplayName(rc), table))
			continue
		}
		m.Statements = append(m.Statements, drop)
	}

	for _, mi := range td.ModifiedIndexes {
		if mi == nil {
			continue
		}
		name := strings.TrimSpace(mi.Old.Name)
		if name == "" {
			m.Unresolved = append(m.Unresolved, fmt.Sprintf("Cannot modify unnamed index on %s automatically (drop/add requires a name).", table))
			continue
		}
		m.Notes = append(m.Notes, fmt.Sprintf("Index %s on %s will be recreated; this can be slow/locking on large tables.", quoteIdent(name), table))
		m.Statements = append(m.Statements,
			fmt.Sprintf("DROP INDEX %s ON %s;", quoteIdent(name), table),
			createIndexMySQL(table, mi.New),
		)
	}

	for _, ridx := range td.RemovedIndexes {
		if ridx == nil {
			continue
		}
		if strings.TrimSpace(ridx.Name) == "" {
			m.Unresolved = append(m.Unresolved, fmt.Sprintf("Cannot drop unnamed index on %s automatically.", table))
			continue
		}
		m.Breaking = append(m.Breaking, fmt.Sprintf("Index %s will be dropped from %s.", quoteIdent(ridx.Name), table))
		m.Statements = append(m.Statements, fmt.Sprintf("DROP INDEX %s ON %s;", quoteIdent(ridx.Name), table))
	}

	// Add columns first (safe and may be needed by subsequent constraint/index changes).
	for _, c := range td.AddedColumns {
		if c == nil {
			continue
		}
		m.Statements = append(m.Statements, fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s;", table, columnDefMySQL(c)))
		if !c.Nullable {
			m.Notes = append(m.Notes, fmt.Sprintf("Added NOT NULL column %s to %s; ensure table is empty or provide a DEFAULT/backfill before enforcing NOT NULL.", quoteIdent(c.Name), table))
		}
	}

	// Modify existing columns.
	for _, ch := range td.ModifiedColumns {
		if ch == nil || ch.New == nil || ch.Old == nil {
			continue
		}
		assessBreakingColumnChange(m, td.Name, ch)
		m.Statements = append(m.Statements, fmt.Sprintf("ALTER TABLE %s MODIFY COLUMN %s;", table, columnDefMySQL(ch.New)))
	}

	// Drop columns last.
	for _, c := range td.RemovedColumns {
		if c == nil {
			continue
		}
		m.Breaking = append(m.Breaking, fmt.Sprintf("Column %s will be dropped from %s (data loss).", quoteIdent(c.Name), table))
		m.Statements = append(m.Statements, fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s;", table, quoteIdent(c.Name)))
	}

	// Add constraints and indexes after columns exist.
	for _, ac := range td.AddedConstraints {
		if ac == nil {
			continue
		}
		add, notes := addConstraintMySQL(table, ac)
		if add == "" {
			m.Unresolved = append(m.Unresolved, fmt.Sprintf("Cannot add constraint %s on %s automatically.", constraintDisplayName(ac), table))
			continue
		}
		m.Statements = append(m.Statements, add)
		m.Notes = append(m.Notes, notes...)
	}

	for _, aidx := range td.AddedIndexes {
		if aidx == nil {
			continue
		}
		stmt := createIndexMySQL(table, aidx)
		if stmt == "" {
			m.Unresolved = append(m.Unresolved, fmt.Sprintf("Cannot add unnamed index on %s automatically.", table))
			continue
		}
		m.Statements = append(m.Statements, stmt)
	}

	for _, mo := range td.ModifiedOptions {
		if mo == nil {
			continue
		}
		stmt, note := alterTableOptionMySQL(table, mo)
		if stmt == "" {
			m.Unresolved = append(m.Unresolved, fmt.Sprintf("Cannot apply table option %s change on %s automatically.", mo.Name, table))
			continue
		}
		m.Statements = append(m.Statements, stmt)
		if note != "" {
			m.Notes = append(m.Notes, note)
		}
	}
}

func assessBreakingColumnChange(m *Migration, tableName string, ch *ColumnChange) {
	table := quoteIdent(tableName)
	oldC, newC := ch.Old, ch.New

	if oldC == nil || newC == nil {
		return
	}

	if !strings.EqualFold(oldC.TypeRaw, newC.TypeRaw) {
		m.Breaking = append(m.Breaking, fmt.Sprintf("Column %s.%s type changes from %q to %q; this may truncate/lose data.", table, quoteIdent(ch.Name), oldC.TypeRaw, newC.TypeRaw))
	}
	if oldC.Nullable && !newC.Nullable {
		m.Breaking = append(m.Breaking, fmt.Sprintf("Column %s.%s becomes NOT NULL; existing NULLs will cause migration failure.", table, quoteIdent(ch.Name)))
	}
	if oldC.IsGenerated != newC.IsGenerated || strings.TrimSpace(oldC.GenerationExpression) != strings.TrimSpace(newC.GenerationExpression) || !strings.EqualFold(strings.TrimSpace(oldC.GenerationStorage), strings.TrimSpace(newC.GenerationStorage)) {
		m.Breaking = append(m.Breaking, fmt.Sprintf("Column %s.%s generation definition changes; verify computed values and dependent indexes.", table, quoteIdent(ch.Name)))
	}
	if oldC.PrimaryKey != newC.PrimaryKey {
		m.Breaking = append(m.Breaking, fmt.Sprintf("Column %s.%s primary key flag changed; primary key changes can fail with duplicates or existing foreign keys.", table, quoteIdent(ch.Name)))
	}
}

func buildCreateTableMySQL(t *Table) (createStmt string, fkAddStatements []string, notes []string) {
	name := quoteIdent(t.Name)

	var lines []string
	for _, c := range t.Columns {
		if c == nil {
			continue
		}
		lines = append(lines, "  "+columnDefMySQL(c))
	}

	var fks []*Constraint
	for _, c := range t.Constraints {
		if c == nil {
			continue
		}
		if c.Type == ForeignKey {
			fks = append(fks, c)
			continue
		}
		line := createTableConstraintLineMySQL(c)
		if line == "" {
			notes = append(notes, fmt.Sprintf("Could not render constraint %s for table %s; you may need to add it manually.", constraintDisplayName(c), name))
			continue
		}
		lines = append(lines, "  "+line)
	}

	for _, idx := range t.Indexes {
		if idx == nil {
			continue
		}
		line := createTableIndexLineMySQL(idx)
		if line == "" {
			notes = append(notes, fmt.Sprintf("Could not render index on table %s; you may need to add it manually.", name))
			continue
		}
		lines = append(lines, "  "+line)
	}

	options := buildTableOptionsMySQL(t)

	create := fmt.Sprintf("CREATE TABLE %s (\n%s\n)%s;", name, strings.Join(lines, ",\n"), options)

	for _, fk := range fks {
		stmt, fkNotes := addConstraintMySQL(name, fk)
		if stmt == "" {
			notes = append(notes, fmt.Sprintf("Could not generate foreign key %s for %s; add it manually.", constraintDisplayName(fk), name))
			continue
		}
		fkAddStatements = append(fkAddStatements, stmt)
		notes = append(notes, fkNotes...)
	}

	return create, fkAddStatements, notes
}

func buildTableOptionsMySQL(t *Table) string {
	parts := make([]string, 0, 6)
	if strings.TrimSpace(t.Engine) != "" {
		parts = append(parts, "ENGINE="+strings.TrimSpace(t.Engine))
	}
	if strings.TrimSpace(t.Charset) != "" {
		parts = append(parts, "DEFAULT CHARSET="+strings.TrimSpace(t.Charset))
	}
	if strings.TrimSpace(t.Collate) != "" {
		parts = append(parts, "COLLATE="+strings.TrimSpace(t.Collate))
	}
	if strings.TrimSpace(t.Comment) != "" {
		parts = append(parts, "COMMENT="+quoteString(t.Comment))
	}
	// Keep AUTO_INCREMENT only if explicitly present.
	if t.AutoIncrement != 0 {
		parts = append(parts, "AUTO_INCREMENT="+strconv.FormatUint(t.AutoIncrement, 10))
	}

	if len(parts) == 0 {
		return ""
	}
	return " " + strings.Join(parts, " ")
}

func columnDefMySQL(c *Column) string {
	var sb strings.Builder
	sb.WriteString(quoteIdent(c.Name))
	sb.WriteString(" ")
	sb.WriteString(strings.TrimSpace(c.TypeRaw))

	// Generated columns (MySQL syntax requires expression + storage).
	if c.IsGenerated {
		expr := strings.TrimSpace(c.GenerationExpression)
		if expr != "" {
			sb.WriteString(" GENERATED ALWAYS AS (")
			sb.WriteString(expr)
			sb.WriteString(")")
			storage := strings.ToUpper(strings.TrimSpace(c.GenerationStorage))
			if storage == "" {
				storage = "VIRTUAL"
			}
			sb.WriteString(" ")
			sb.WriteString(storage)
		}
	}

	if c.Nullable {
		sb.WriteString(" NULL")
	} else {
		sb.WriteString(" NOT NULL")
	}

	if c.AutoIncrement {
		sb.WriteString(" AUTO_INCREMENT")
	}

	if strings.TrimSpace(c.Charset) != "" {
		sb.WriteString(" CHARACTER SET ")
		sb.WriteString(strings.TrimSpace(c.Charset))
	}
	if strings.TrimSpace(c.Collate) != "" {
		sb.WriteString(" COLLATE ")
		sb.WriteString(strings.TrimSpace(c.Collate))
	}

	if c.DefaultValue != nil {
		sb.WriteString(" DEFAULT ")
		sb.WriteString(formatSQLValue(*c.DefaultValue))
	}
	if c.OnUpdate != nil {
		sb.WriteString(" ON UPDATE ")
		sb.WriteString(formatSQLValue(*c.OnUpdate))
	}
	if strings.TrimSpace(c.Comment) != "" {
		sb.WriteString(" COMMENT ")
		sb.WriteString(quoteString(c.Comment))
	}

	return sb.String()
}

func createTableConstraintLineMySQL(c *Constraint) string {
	cols := formatIdentList(c.Columns)
	switch c.Type {
	case PrimaryKey:
		return fmt.Sprintf("PRIMARY KEY %s", cols)
	case Unique:
		if strings.TrimSpace(c.Name) != "" {
			return fmt.Sprintf("CONSTRAINT %s UNIQUE KEY %s", quoteIdent(c.Name), cols)
		}
		return fmt.Sprintf("UNIQUE KEY %s", cols)
	case Check:
		expr := strings.TrimSpace(c.CheckExpression)
		if expr == "" {
			return ""
		}
		if strings.TrimSpace(c.Name) != "" {
			return fmt.Sprintf("CONSTRAINT %s CHECK (%s)", quoteIdent(c.Name), expr)
		}
		return fmt.Sprintf("CHECK (%s)", expr)
	default:
		return ""
	}
}

func createTableIndexLineMySQL(i *Index) string {
	cols := formatIdentList(i.Columns)
	name := strings.TrimSpace(i.Name)
	if name == "" {
		// MySQL requires a name for standalone KEY definitions.
		return ""
	}

	typ := strings.ToUpper(strings.TrimSpace(i.Type))
	switch {
	case i.Unique:
		return fmt.Sprintf("UNIQUE KEY %s %s", quoteIdent(name), cols)
	case typ == "FULLTEXT":
		return fmt.Sprintf("FULLTEXT KEY %s %s", quoteIdent(name), cols)
	default:
		return fmt.Sprintf("KEY %s %s", quoteIdent(name), cols)
	}
}

func addConstraintMySQL(table string, c *Constraint) (stmt string, notes []string) {
	if c == nil {
		return "", nil
	}
	cols := formatIdentList(c.Columns)

	switch c.Type {
	case PrimaryKey:
		if len(c.Columns) == 0 {
			return "", nil
		}
		notes = append(notes, fmt.Sprintf("Adding PRIMARY KEY on %s; ensure there are no duplicates/NULLs in the key columns.", table))
		return fmt.Sprintf("ALTER TABLE %s ADD PRIMARY KEY %s;", table, cols), notes
	case Unique:
		if len(c.Columns) == 0 {
			return "", nil
		}
		notes = append(notes, fmt.Sprintf("Adding UNIQUE constraint on %s; ensure there are no duplicate values in %s.", table, cols))
		if strings.TrimSpace(c.Name) != "" {
			return fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s UNIQUE %s;", table, quoteIdent(c.Name), cols), notes
		}
		return fmt.Sprintf("ALTER TABLE %s ADD UNIQUE %s;", table, cols), notes
	case ForeignKey:
		if len(c.Columns) == 0 || strings.TrimSpace(c.ReferencedTable) == "" || len(c.ReferencedColumns) == 0 {
			return "", nil
		}
		notes = append(notes, fmt.Sprintf("Adding FOREIGN KEY on %s; ensure existing rows satisfy referential integrity.", table))
		var sb strings.Builder
		sb.WriteString("ALTER TABLE ")
		sb.WriteString(table)
		sb.WriteString(" ADD ")
		if strings.TrimSpace(c.Name) != "" {
			sb.WriteString("CONSTRAINT ")
			sb.WriteString(quoteIdent(c.Name))
			sb.WriteString(" ")
		}
		sb.WriteString("FOREIGN KEY ")
		sb.WriteString(cols)
		sb.WriteString(" REFERENCES ")
		sb.WriteString(quoteIdent(c.ReferencedTable))
		sb.WriteString(" ")
		sb.WriteString(formatIdentList(c.ReferencedColumns))
		if strings.TrimSpace(c.OnDelete) != "" {
			sb.WriteString(" ON DELETE ")
			sb.WriteString(strings.TrimSpace(c.OnDelete))
		}
		if strings.TrimSpace(c.OnUpdate) != "" {
			sb.WriteString(" ON UPDATE ")
			sb.WriteString(strings.TrimSpace(c.OnUpdate))
		}
		sb.WriteString(";")
		return sb.String(), notes
	case Check:
		expr := strings.TrimSpace(c.CheckExpression)
		if expr == "" {
			return "", nil
		}
		notes = append(notes, fmt.Sprintf("Adding CHECK constraint on %s; ensure existing rows satisfy: %s.", table, expr))
		if strings.TrimSpace(c.Name) != "" {
			return fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s CHECK (%s);", table, quoteIdent(c.Name), expr), notes
		}
		return fmt.Sprintf("ALTER TABLE %s ADD CHECK (%s);", table, expr), notes
	default:
		return "", nil
	}
}

func dropConstraintMySQL(table string, c *Constraint) (stmt string, ok bool) {
	if c == nil {
		return "", false
	}

	switch c.Type {
	case PrimaryKey:
		return fmt.Sprintf("ALTER TABLE %s DROP PRIMARY KEY;", table), true
	case ForeignKey:
		if strings.TrimSpace(c.Name) == "" {
			return "", false
		}
		return fmt.Sprintf("ALTER TABLE %s DROP FOREIGN KEY %s;", table, quoteIdent(c.Name)), true
	case Unique:
		if strings.TrimSpace(c.Name) == "" {
			return "", false
		}
		// In MySQL, UNIQUE constraints are backed by indexes.
		return fmt.Sprintf("ALTER TABLE %s DROP INDEX %s;", table, quoteIdent(c.Name)), true
	case Check:
		if strings.TrimSpace(c.Name) == "" {
			return "", false
		}
		return fmt.Sprintf("ALTER TABLE %s DROP CHECK %s;", table, quoteIdent(c.Name)), true
	default:
		return "", false
	}
}

func createIndexMySQL(table string, idx *Index) string {
	if idx == nil {
		return ""
	}
	name := strings.TrimSpace(idx.Name)
	if name == "" {
		return ""
	}
	cols := formatIdentList(idx.Columns)
	typ := strings.ToUpper(strings.TrimSpace(idx.Type))

	if idx.Unique {
		return fmt.Sprintf("CREATE UNIQUE INDEX %s ON %s %s;", quoteIdent(name), table, cols)
	}
	if typ == "FULLTEXT" {
		return fmt.Sprintf("CREATE FULLTEXT INDEX %s ON %s %s;", quoteIdent(name), table, cols)
	}
	return fmt.Sprintf("CREATE INDEX %s ON %s %s;", quoteIdent(name), table, cols)
}

func alterTableOptionMySQL(table string, ch *TableOptionChange) (stmt string, note string) {
	if ch == nil {
		return "", ""
	}
	name := strings.TrimSpace(ch.Name)
	if name == "" {
		return "", ""
	}

	switch strings.ToUpper(name) {
	case "ENGINE":
		if strings.TrimSpace(ch.New) == "" {
			return "", ""
		}
		note = fmt.Sprintf("Changing ENGINE for %s can be slow and may rebuild the table.", table)
		return fmt.Sprintf("ALTER TABLE %s ENGINE=%s;", table, strings.TrimSpace(ch.New)), note
	case "AUTO_INCREMENT":
		if strings.TrimSpace(ch.New) == "" {
			return "", ""
		}
		return fmt.Sprintf("ALTER TABLE %s AUTO_INCREMENT=%s;", table, strings.TrimSpace(ch.New)), ""
	case "CHARSET":
		if strings.TrimSpace(ch.New) == "" {
			return "", ""
		}
		note = fmt.Sprintf("Changing DEFAULT CHARSET for %s may require data conversion; verify text columns.", table)
		return fmt.Sprintf("ALTER TABLE %s DEFAULT CHARSET=%s;", table, strings.TrimSpace(ch.New)), note
	case "COLLATE":
		if strings.TrimSpace(ch.New) == "" {
			return "", ""
		}
		note = fmt.Sprintf("Changing COLLATE for %s may affect comparisons and indexes.", table)
		return fmt.Sprintf("ALTER TABLE %s COLLATE=%s;", table, strings.TrimSpace(ch.New)), note
	case "COMMENT":
		return fmt.Sprintf("ALTER TABLE %s COMMENT=%s;", table, quoteString(ch.New)), ""
	default:

		newV := strings.TrimSpace(ch.New)
		if newV == "" {
			return "", ""
		}
		if looksNumeric(newV) {
			return fmt.Sprintf("ALTER TABLE %s %s=%s;", table, name, newV), ""
		}

		return fmt.Sprintf("ALTER TABLE %s %s=%s;", table, name, quoteString(newV)), ""
	}
}

func constraintDisplayName(c *Constraint) string {
	if c == nil {
		return "(nil)"
	}
	if strings.TrimSpace(c.Name) != "" {
		return quoteIdent(c.Name)
	}
	return string(c.Type)
}

func formatIdentList(cols []string) string {
	parts := make([]string, 0, len(cols))
	for _, c := range cols {
		c = strings.TrimSpace(c)
		if c == "" {
			continue
		}
		parts = append(parts, quoteIdent(c))
	}
	return "(" + strings.Join(parts, ", ") + ")"
}

func quoteIdent(s string) string {
	s = strings.TrimSpace(s)
	s = strings.ReplaceAll(s, "`", "``")
	return "`" + s + "`"
}

func quoteString(s string) string {
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, "'", "\\'")
	return "'" + s + "'"
}

func looksNumeric(s string) bool {
	_, err := strconv.ParseFloat(s, 64)
	return err == nil
}

func formatSQLValue(v string) string {
	v = strings.TrimSpace(v)
	if v == "" {
		return "''"
	}
	up := strings.ToUpper(v)

	if up == "NULL" || up == "CURRENT_TIMESTAMP" || up == "CURRENT_DATE" || up == "CURRENT_TIME" {
		return up
	}
	if looksNumeric(v) {
		return v
	}

	if strings.ContainsAny(v, "()") {
		return v
	}
	return quoteString(v)
}

func dedupeStable(items []string) []string {
	seen := make(map[string]struct{}, len(items))
	out := make([]string, 0, len(items))
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
