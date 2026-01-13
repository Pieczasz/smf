package mysql

import (
	"fmt"
	"hash/fnv"
	"smf/core"
	"smf/dialect"
	"smf/diff"
	"smf/migration"
	"smf/parser/mysql"
	"strings"
)

const backupSuffixPrefix = "__smf_backup_"

func init() {
	dialect.RegisterDialect(dialect.MySQL, func() dialect.Dialect {
		return NewMySQLDialect()
	})
}

type Dialect struct {
	generator *Generator
	parser    *mysql.Parser
}

func NewMySQLDialect() *Dialect {
	return &Dialect{
		generator: NewMySQLGenerator(),
		parser:    mysql.NewParser(),
	}
}

func (d *Dialect) Name() dialect.Type {
	return dialect.MySQL
}

func (d *Dialect) Generator() dialect.Generator {
	return d.generator
}

func (d *Dialect) Parser() dialect.Parser {
	return d.parser
}

type Generator struct{}

func NewMySQLGenerator() *Generator {
	return &Generator{}
}

func (g *Generator) GenerateMigration(schemaDiff *diff.SchemaDiff) *migration.Migration {
	opts := dialect.DefaultMigrationOptions(dialect.MySQL)
	opts.IncludeUnsafe = true
	return g.GenerateMigrationWithOptions(schemaDiff, opts)
}

func (g *Generator) GenerateMigrationWithOptions(schemaDiff *diff.SchemaDiff, opts dialect.MigrationOptions) *migration.Migration {
	m := &migration.Migration{}
	if schemaDiff == nil {
		m.AddNote("No diff provided; nothing to migrate.")
		return m
	}

	analyzer := diff.NewBreakingChangeAnalyzer()
	breakingChanges := analyzer.Analyze(schemaDiff)
	for _, bc := range breakingChanges {
		switch bc.Severity {
		case diff.SeverityCritical, diff.SeverityBreaking:
			m.AddBreaking(fmt.Sprintf("[%s] %s.%s: %s", bc.Severity, bc.Table, bc.Object, bc.Description))
		case diff.SeverityWarning:
			m.AddNote(fmt.Sprintf("[WARNING] %s.%s: %s", bc.Table, bc.Object, bc.Description))
		case diff.SeverityInfo:
		}

		for _, rec := range migrationRecommendations(bc) {
			m.AddNote(rec)
		}
	}

	if !opts.IncludeUnsafe {
		m.AddNote("Safe mode: destructive drops are avoided (tables/columns are renamed to __smf_backup_* instead of dropped) to enable a reliable rollback.")
	}

	var pendingFKs []string
	var pendingFKRollback []string

	for _, t := range schemaDiff.AddedTables {
		if t == nil {
			continue
		}
		create, fks := g.GenerateCreateTable(t)
		m.AddStatementWithRollback(create, g.GenerateDropTable(t))
		pendingFKs = append(pendingFKs, fks...)

		table := g.QuoteIdentifier(t.Name)
		for _, c := range t.Constraints {
			if c == nil || c.Type != core.ConstraintForeignKey {
				continue
			}
			rb := g.dropConstraint(table, c)
			if strings.TrimSpace(rb) != "" {
				pendingFKRollback = append(pendingFKRollback, rb)
			}
		}
	}

	for _, td := range schemaDiff.ModifiedTables {
		if td == nil {
			continue
		}
		stmts, rollback, fkAdds, fkRollback := g.generateAlterTableWithOptions(td, opts)

		pairCount := len(stmts)
		if len(rollback) < pairCount {
			pairCount = len(rollback)
		}
		for i := 0; i < pairCount; i++ {
			m.AddStatementWithRollback(stmts[i], rollback[i])
		}

		for i := pairCount; i < len(stmts); i++ {
			m.AddStatement(stmts[i])
		}

		for i := pairCount; i < len(rollback); i++ {
			m.AddRollbackStatement(rollback[i])
		}

		pendingFKs = append(pendingFKs, fkAdds...)
		pendingFKRollback = append(pendingFKRollback, fkRollback...)
	}

	if len(pendingFKs) > 0 {
		m.AddNote("Foreign keys added after table creation to avoid dependency issues.")

		for i, stmt := range pendingFKs {
			if i < len(pendingFKRollback) {
				rb := pendingFKRollback[i]
				if strings.TrimSpace(rb) != "" {
					m.AddStatementWithRollback(stmt, rb)
					continue
				}
			}
			m.AddStatement(stmt)
		}

		for i := len(pendingFKs); i < len(pendingFKRollback); i++ {
			rb := pendingFKRollback[i]
			if strings.TrimSpace(rb) != "" {
				m.AddRollbackStatement(rb)
			}
		}
	}

	for _, t := range schemaDiff.RemovedTables {
		if t == nil {
			continue
		}
		if opts.IncludeUnsafe {
			m.AddStatementWithRollback(g.GenerateDropTable(t), fmt.Sprintf("-- cannot auto-restore dropped table %s; restore from backup", g.QuoteIdentifier(t.Name)))
			continue
		}
		backup := g.safeBackupName(t.Name)
		up := fmt.Sprintf("RENAME TABLE %s TO %s;", g.QuoteIdentifier(t.Name), g.QuoteIdentifier(backup))
		down := fmt.Sprintf("RENAME TABLE %s TO %s;", g.QuoteIdentifier(backup), g.QuoteIdentifier(t.Name))
		m.AddStatementWithRollback(up, down)
	}

	if hasPotentiallyLockingStatements(m.Plan()) {
		m.AddNote("Lock-time warning: ALTER TABLE / index changes may lock or rebuild tables; for large tables consider online schema change tools and off-peak execution.")
	}

	m.Dedupe()

	return m
}

func (g *Generator) GenerateCreateTable(t *core.Table) (string, []string) {
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

func (g *Generator) GenerateDropTable(t *core.Table) string {
	return fmt.Sprintf("DROP TABLE %s;", g.QuoteIdentifier(t.Name))
}

func (g *Generator) GenerateAlterTable(td *diff.TableDiff) []string {
	stmts, fkAdds := g.generateAlterTable(td)
	return append(stmts, fkAdds...)
}

func (g *Generator) QuoteIdentifier(name string) string {
	name = strings.TrimSpace(name)
	name = strings.ReplaceAll(name, "`", "``")
	return "`" + name + "`"
}

func (g *Generator) QuoteString(value string) string {
	value = strings.ReplaceAll(value, "\\", "\\\\")
	value = strings.ReplaceAll(value, "'", "\\'")
	return "'" + value + "'"
}

// Helpers
func (g *Generator) safeBackupName(name string) string {
	base := strings.TrimSpace(name)
	h := fnv.New64a()
	_, _ = h.Write([]byte(base))
	suffix := fmt.Sprintf("%s%016x", backupSuffixPrefix, h.Sum64())

	const mysqlMaxIdentLen = 64
	if len(base)+len(suffix) > mysqlMaxIdentLen {
		maxBase := mysqlMaxIdentLen - len(suffix)
		if maxBase < 0 {
			maxBase = 0
		}
		if len(base) > maxBase {
			base = base[:maxBase]
		}
	}

	if base == "" {
		return suffix
	}
	return base + suffix
}

func hasPotentiallyLockingStatements(plan []core.Operation) bool {
	for _, op := range plan {
		if op.Kind != core.OperationSQL {
			continue
		}
		s := strings.TrimSpace(op.SQL)
		if s == "" {
			continue
		}
		u := strings.ToUpper(strings.TrimSpace(s))
		if strings.HasPrefix(u, "ALTER TABLE") || strings.HasPrefix(u, "CREATE INDEX") || strings.HasPrefix(u, "DROP INDEX") {
			return true
		}
	}
	return false
}
