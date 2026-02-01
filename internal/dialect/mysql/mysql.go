// Package mysql provides MySQL dialect support for schema migration,
// rollback generation, formatting, and migration.
package mysql

import (
	"fmt"
	"hash/fnv"
	"strings"

	"smf/internal/core"
	"smf/internal/dialect"
	"smf/internal/diff"
	"smf/internal/migration"
	"smf/internal/parser/mysql"
)

// Instead of removing the table, in safe mode we rename it to a backup table, so
// that rollback is possible, and all data is preserved.
const backupSuffixPrefix = "__smf_backup_"

const mysqlMaxIdentLen = 64

func init() {
	dialect.RegisterDialect(dialect.MySQL, func() dialect.Dialect {
		return NewMySQLDialect()
	})
}

// Dialect represents the MySQL dialect struct. With migration generator
// and parser.
type Dialect struct {
	generator *Generator
	parser    *mysql.Parser
}

// NewMySQLDialect initializes a new MySQL dialect instance.
func NewMySQLDialect() *Dialect {
	return &Dialect{
		generator: NewMySQLGenerator(),
		parser:    mysql.NewParser(),
	}
}

// Name returns the name of the MySQL dialect.
func (d *Dialect) Name() dialect.Type {
	return dialect.MySQL
}

// Generator returns the migration generator for the MySQL dialect.
func (d *Dialect) Generator() dialect.Generator {
	return d.generator
}

// Parser returns the schema parser for the MySQL dialect.
func (d *Dialect) Parser() dialect.Parser {
	return d.parser
}

// Generator is a stateless struct for generating MySQL migrations.
type Generator struct{}

// NewMySQLGenerator initializes a new MySQL migration generator instance.
func NewMySQLGenerator() *Generator {
	return &Generator{}
}

// GenerateMigration generates a migration for the given schema diff in safe mode.
// Safe mode renames drops instead of executing them, preserving data and enabling rollback.
func (g *Generator) GenerateMigration(schemaDiff *diff.SchemaDiff) *migration.Migration {
	opts := dialect.DefaultMigrationOptions(dialect.MySQL)
	opts.IncludeUnsafe = false
	return g.GenerateMigrationWithOptions(schemaDiff, opts)
}

// GenerateMigrationWithOptions generates a migration for the given schema diff with the given options.
// A user provides options to customize the migration process.
func (g *Generator) GenerateMigrationWithOptions(schemaDiff *diff.SchemaDiff, opts dialect.MigrationOptions) *migration.Migration {
	m := &migration.Migration{}

	g.addBreakingChangeWarnings(m, schemaDiff)

	if !opts.IncludeUnsafe {
		m.AddNote("Safe mode: destructive drops are avoided (tables/columns are renamed to __smf_backup_* instead of dropped) to enable a reliable rollback.")
	}

	pendingFKs, pendingFKRollback := g.processAddedTables(m, schemaDiff)
	fks, fkRB := g.processModifiedTables(m, schemaDiff, &opts)
	pendingFKs = append(pendingFKs, fks...)
	pendingFKRollback = append(pendingFKRollback, fkRB...)

	g.addPendingForeignKeys(m, pendingFKs, pendingFKRollback)
	g.processRemovedTables(m, schemaDiff, &opts)

	if hasPotentiallyLockingStatements(m.Plan()) {
		m.AddNote("Lock-time warning: ALTER TABLE / index changes may lock or rebuild tables; for large tables consider online schema change tools and off-peak execution.")
	}

	m.Dedupe()
	return m
}

func (g *Generator) addBreakingChangeWarnings(m *migration.Migration, schemaDiff *diff.SchemaDiff) {
	analyzer := diff.NewBreakingChangeAnalyzer()
	breakingChanges := analyzer.Analyze(schemaDiff)
	for i := range breakingChanges {
		bc := &breakingChanges[i]
		switch bc.Severity {
		case diff.SeverityCritical, diff.SeverityBreaking:
			m.AddBreaking(fmt.Sprintf("[%s] %s.%s: %s", bc.Severity, bc.Table, bc.Object, bc.Description))
		case diff.SeverityWarning:
			m.AddNote(fmt.Sprintf("[WARNING] %s.%s: %s", bc.Table, bc.Object, bc.Description))
		case diff.SeverityInfo:
		}

		for _, rec := range migrationRecommendations(*bc) {
			m.AddNote(rec)
		}
	}
}

func (g *Generator) processAddedTables(m *migration.Migration, schemaDiff *diff.SchemaDiff) ([]string, []string) {
	estimatedFKs := len(schemaDiff.AddedTables) * 2
	pendingFKs := make([]string, 0, estimatedFKs)
	pendingFKRollback := make([]string, 0, estimatedFKs)

	for _, at := range schemaDiff.AddedTables {
		create, fks := g.GenerateCreateTable(at)
		m.AddStatementWithRollback(create, g.GenerateDropTable(at))
		pendingFKs = append(pendingFKs, fks...)

		table := g.QuoteIdentifier(at.Name)
		for _, c := range at.Constraints {
			if c.Type != core.ConstraintForeignKey {
				continue
			}
			rb := g.dropConstraint(table, c)
			if strings.TrimSpace(rb) != "" {
				pendingFKRollback = append(pendingFKRollback, rb)
			}
		}
	}
	return pendingFKs, pendingFKRollback
}

func (g *Generator) processModifiedTables(m *migration.Migration, schemaDiff *diff.SchemaDiff, opts *dialect.MigrationOptions) ([]string, []string) {
	var pendingFKs, pendingFKRollback []string

	for _, td := range schemaDiff.ModifiedTables {
		result := g.generateAlterTable(td, opts)

		pairCount := min(len(result.Statements), len(result.Rollback))
		for i := range pairCount {
			m.AddStatementWithRollback(result.Statements[i], result.Rollback[i])
		}
		for i := pairCount; i < len(result.Statements); i++ {
			m.AddStatement(result.Statements[i])
		}

		pendingFKs = append(pendingFKs, result.FKStatements...)
		pendingFKRollback = append(pendingFKRollback, result.FKRollback...)
	}
	return pendingFKs, pendingFKRollback
}

func (g *Generator) addPendingForeignKeys(m *migration.Migration, pendingFKs, pendingFKRollback []string) {
	if len(pendingFKs) == 0 {
		return
	}

	m.AddNote("Foreign keys added after table creation to avoid dependency issues.")
	for i, stmt := range pendingFKs {
		if i < len(pendingFKRollback) {
			m.AddStatementWithRollback(stmt, pendingFKRollback[i])
		} else {
			m.AddStatement(stmt)
		}
	}
}

func (g *Generator) processRemovedTables(m *migration.Migration, schemaDiff *diff.SchemaDiff, opts *dialect.MigrationOptions) {
	for _, t := range schemaDiff.RemovedTables {
		if opts.IncludeUnsafe {
			m.AddStatementWithRollback(g.GenerateDropTable(t), fmt.Sprintf("-- cannot auto-restore dropped table %s; restore from backup", g.QuoteIdentifier(t.Name)))
			continue
		}
		backup := g.safeBackupName(t.Name)
		up := fmt.Sprintf("RENAME TABLE %s TO %s;", g.QuoteIdentifier(t.Name), g.QuoteIdentifier(backup))
		down := fmt.Sprintf("RENAME TABLE %s TO %s;", g.QuoteIdentifier(backup), g.QuoteIdentifier(t.Name))
		m.AddStatementWithRollback(up, down)
	}
}

// GenerateCreateTable generate an SQL statement to create a table, depending on Table struct representation.
func (g *Generator) GenerateCreateTable(t *core.Table) (string, []string) {
	name := g.QuoteIdentifier(t.Name)

	lines, fks := g.buildTableDefinitionLines(t)
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

func (g *Generator) buildTableDefinitionLines(t *core.Table) ([]string, []*core.Constraint) {
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

	return lines, fks
}

// GenerateDropTable generate an SQL statement to drop a table.
func (g *Generator) GenerateDropTable(t *core.Table) string {
	return fmt.Sprintf("DROP TABLE %s;", g.QuoteIdentifier(t.Name))
}

// GenerateAlterTable generates SQL statements to alter a table using default options.
func (g *Generator) GenerateAlterTable(td *diff.TableDiff) []string {
	result := g.generateAlterTable(td, nil)
	return result.AllStatements()
}

// QuoteIdentifier is a function used for quote identification inside an SQL dialect.
func (g *Generator) QuoteIdentifier(name string) string {
	name = strings.TrimSpace(name)
	name = strings.ReplaceAll(name, "`", "``")
	return "`" + name + "`"
}

// QuoteString is a function used for quote string inside an SQL dialect.
func (g *Generator) QuoteString(value string) string {
	var b strings.Builder
	b.Grow(len(value) + len(value)/10 + 2)

	b.WriteByte('\'')
	for _, char := range value {
		switch char {
		case '\'':
			b.WriteString("''")
		case '\\': // Backslash escaped
			b.WriteString(`\\`)
		case '\x00': // NUL byte
			b.WriteString(`\0`)
		case '\n': // Newline
			b.WriteString(`\n`)
		case '\r': // Carriage return
			b.WriteString(`\r`)
		case '\x1A': // Ctrl+Z
			b.WriteString(`\Z`)
		default:
			b.WriteRune(char)
		}
	}
	b.WriteByte('\'')
	return b.String()
}

// Helpers
func (g *Generator) safeBackupName(name string) string {
	base := strings.TrimSpace(name)

	// Create a non-cryptographic hash (FNV-1a) of the name
	h := fnv.New64a()
	_, _ = h.Write([]byte(base))

	// 1. h.Sum64(): Gets the calculated hash as a generic unsigned 64-bit integer (uint64).
	// 2. %s: Inserts the 'backupSuffixPrefix' string.
	// 3. %016x: Formats the uint64 hash as a Hexadecimal string.
	//    - 'x': Hex format (base 16).
	//    - '16': Minimum width of 16 characters.
	//    - '0': Pad with leading zeros if the hash is shorter than 16 chars.
	suffix := fmt.Sprintf("%s%016x", backupSuffixPrefix, h.Sum64())

	// Ensure the total length does not exceed MySQL's limit (usually 64 bytes)
	if len(base)+len(suffix) > mysqlMaxIdentLen {
		maxBase := max(mysqlMaxIdentLen-len(suffix), 0)
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
	for i := range plan {
		op := &plan[i]
		if op.Kind != core.OperationSQL {
			continue
		}
		s := strings.TrimSpace(op.SQL)
		if s == "" {
			continue
		}
		if hasPrefixFoldCI(s, "ALTER TABLE") || hasPrefixFoldCI(s, "CREATE INDEX") || hasPrefixFoldCI(s, "DROP INDEX") {
			return true
		}
	}
	return false
}

func hasPrefixFoldCI(s, prefix string) bool {
	if len(s) < len(prefix) {
		return false
	}
	return strings.EqualFold(s[:len(prefix)], prefix)
}
