package mysql

import (
	"fmt"
	"strings"

	"smf/internal/core"
	"smf/internal/dialect"
	"smf/internal/diff"
)

// AlterTableResult holds all generated statements for an ALTER TABLE operation.
// Separating regular statements from foreign key statements ensures proper
// ordering during migration execution (FKs are added last to avoid dependency issues).
type AlterTableResult struct {
	Statements   []string
	Rollback     []string
	FKStatements []string
	FKRollback   []string
}

// Add appends a statement and its rollback to the result.
func (r *AlterTableResult) Add(up, down string) {
	r.Statements = append(r.Statements, up)
	if strings.TrimSpace(down) != "" {
		r.Rollback = append(r.Rollback, down)
	}
}

// AddFK appends a foreign key statement and its rollback to the result.
func (r *AlterTableResult) AddFK(up, down string) {
	r.FKStatements = append(r.FKStatements, up)
	if strings.TrimSpace(down) != "" {
		r.FKRollback = append(r.FKRollback, down)
	}
}

// AllStatements returns all statements (regular + FK) in execution order.
func (r *AlterTableResult) AllStatements() []string {
	all := make([]string, 0, len(r.Statements)+len(r.FKStatements))
	all = append(all, r.Statements...)
	return append(all, r.FKStatements...)
}

func migrationRecommendations(bc diff.BreakingChange) []string {
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

func (g *Generator) generateAlterTable(td *diff.TableDiff, opts *dialect.MigrationOptions) *AlterTableResult {
	if td == nil {
		return &AlterTableResult{}
	}
	if opts == nil {
		defaultOpts := dialect.DefaultMigrationOptions(dialect.MySQL)
		opts = &defaultOpts
	}

	table := g.QuoteIdentifier(td.Name)
	result := &AlterTableResult{}

	g.generateConstraintDrops(td, table, result)
	g.generateIndexDrops(td, table, result)

	g.generateColumnChanges(td, table, opts, result)

	g.generateOptionChanges(td, table, result)

	g.generateIndexCreates(td, table, result)

	g.generateConstraintAdds(td, table, result)

	return result
}

func (g *Generator) generateConstraintDrops(td *diff.TableDiff, table string, result *AlterTableResult) {
	for _, mc := range td.ModifiedConstraints {
		if mc == nil || mc.Old == nil {
			continue
		}
		if drop := g.dropConstraint(table, mc.Old); drop != "" {
			result.Add(drop, g.addConstraint(table, mc.Old))
		}
	}

	for _, rc := range td.RemovedConstraints {
		if rc == nil {
			continue
		}
		if drop := g.dropConstraint(table, rc); drop != "" {
			result.Add(drop, g.addConstraint(table, rc))
		}
	}
}

func (g *Generator) generateIndexDrops(td *diff.TableDiff, table string, result *AlterTableResult) {
	for _, mi := range td.ModifiedIndexes {
		if mi == nil || mi.Old == nil || strings.TrimSpace(mi.Old.Name) == "" {
			continue
		}
		up := fmt.Sprintf("DROP INDEX %s ON %s;", g.QuoteIdentifier(mi.Old.Name), table)
		down := g.createIndex(table, mi.Old)
		result.Add(up, down)
	}

	for _, ri := range td.RemovedIndexes {
		if ri == nil || strings.TrimSpace(ri.Name) == "" {
			continue
		}
		up := fmt.Sprintf("DROP INDEX %s ON %s;", g.QuoteIdentifier(ri.Name), table)
		down := g.createIndex(table, ri)
		result.Add(up, down)
	}
}

func (g *Generator) generateColumnChanges(td *diff.TableDiff, table string, opts *dialect.MigrationOptions, result *AlterTableResult) {
	g.generateColumnRenames(td, table, result)
	g.generateColumnAdditions(td, table, result)
	g.generateColumnModifications(td, table, result)
	g.generateColumnRemovals(td, table, opts, result)
}

func (g *Generator) generateColumnRenames(td *diff.TableDiff, table string, result *AlterTableResult) {
	for _, rc := range td.RenamedColumns {
		if rc == nil || rc.Old == nil || rc.New == nil {
			continue
		}
		up := fmt.Sprintf("ALTER TABLE %s CHANGE COLUMN %s %s;", table, g.QuoteIdentifier(rc.Old.Name), g.columnDefinition(rc.New))
		down := fmt.Sprintf("ALTER TABLE %s CHANGE COLUMN %s %s;", table, g.QuoteIdentifier(rc.New.Name), g.columnDefinition(rc.Old))
		result.Add(up, down)
	}
}

func (g *Generator) generateColumnAdditions(td *diff.TableDiff, table string, result *AlterTableResult) {
	for _, c := range td.AddedColumns {
		if c == nil {
			continue
		}
		up := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s;", table, g.columnDefinition(c))
		down := fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s;", table, g.QuoteIdentifier(c.Name))
		result.Add(up, down)
	}
}

func (g *Generator) generateColumnModifications(td *diff.TableDiff, table string, result *AlterTableResult) {
	for _, ch := range td.ModifiedColumns {
		if ch == nil || ch.New == nil || ch.Old == nil {
			continue
		}
		up := fmt.Sprintf("ALTER TABLE %s MODIFY COLUMN %s;", table, g.columnDefinition(ch.New))
		down := fmt.Sprintf("ALTER TABLE %s MODIFY COLUMN %s;", table, g.columnDefinition(ch.Old))
		result.Add(up, down)
	}
}

func (g *Generator) generateColumnRemovals(td *diff.TableDiff, table string, opts *dialect.MigrationOptions, result *AlterTableResult) {
	for _, rc := range td.RemovedColumns {
		if rc == nil {
			continue
		}

		if opts.IncludeUnsafe {
			up := fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s;", table, g.QuoteIdentifier(rc.Name))
			down := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s;", table, g.columnDefinition(rc))
			result.Add(up, down)
			continue
		}

		backupName := g.safeBackupName(rc.Name)
		backupCol := *rc
		backupCol.Name = backupName
		up := fmt.Sprintf("ALTER TABLE %s CHANGE COLUMN %s %s;", table, g.QuoteIdentifier(rc.Name), g.columnDefinition(&backupCol))
		down := fmt.Sprintf("ALTER TABLE %s CHANGE COLUMN %s %s;", table, g.QuoteIdentifier(backupName), g.columnDefinition(rc))
		result.Add(up, down)
	}
}

func (g *Generator) generateOptionChanges(td *diff.TableDiff, table string, result *AlterTableResult) {
	for _, mo := range td.ModifiedOptions {
		if mo == nil {
			continue
		}
		up := g.alterOption(table, mo)
		if up == "" {
			continue
		}
		down := g.alterOption(table, &diff.TableOptionChange{Name: mo.Name, Old: mo.New, New: mo.Old})
		result.Add(up, down)
	}
}

func (g *Generator) generateIndexCreates(td *diff.TableDiff, table string, result *AlterTableResult) {
	for _, mi := range td.ModifiedIndexes {
		if mi == nil || mi.New == nil {
			continue
		}
		up := g.createIndex(table, mi.New)
		down := ""
		if strings.TrimSpace(mi.New.Name) != "" {
			down = fmt.Sprintf("DROP INDEX %s ON %s;", g.QuoteIdentifier(mi.New.Name), table)
		}
		result.Add(up, down)
	}

	for _, ai := range td.AddedIndexes {
		if ai == nil {
			continue
		}
		up := g.createIndex(table, ai)
		down := ""
		if strings.TrimSpace(ai.Name) != "" {
			down = fmt.Sprintf("DROP INDEX %s ON %s;", g.QuoteIdentifier(ai.Name), table)
		}
		result.Add(up, down)
	}
}

func (g *Generator) generateConstraintAdds(td *diff.TableDiff, table string, result *AlterTableResult) {
	for _, mc := range td.ModifiedConstraints {
		if mc == nil || mc.New == nil {
			continue
		}
		addStmt := g.addConstraint(table, mc.New)
		if addStmt == "" {
			continue
		}

		if mc.New.Type == core.ConstraintForeignKey {
			result.AddFK(addStmt, g.dropConstraint(table, mc.New))
		} else {
			result.Add(addStmt, g.dropConstraint(table, mc.New))
		}
	}

	for _, ac := range td.AddedConstraints {
		if ac == nil {
			continue
		}
		addStmt := g.addConstraint(table, ac)
		if addStmt == "" {
			continue
		}

		if ac.Type == core.ConstraintForeignKey {
			result.AddFK(addStmt, g.dropConstraint(table, ac))
		} else {
			result.Add(addStmt, g.dropConstraint(table, ac))
		}
	}
}
