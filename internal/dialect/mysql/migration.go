package mysql

import (
	"fmt"
	"strings"

	"smf/internal/core"
	"smf/internal/dialect"
	"smf/internal/diff"
)

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

func (g *Generator) generateAlterTableWithOptions(td *diff.TableDiff, opts dialect.MigrationOptions) ([]string, []string, []string, []string) {
	table := g.QuoteIdentifier(td.Name)
	var stmts []string
	var rollback []string
	var fkAdds []string
	var fkRollback []string

	add := func(up, down string) {
		stmts = append(stmts, up)
		if strings.TrimSpace(down) != "" {
			rollback = append(rollback, down)
		}
	}
	addFK := func(up, down string) {
		fkAdds = append(fkAdds, up)
		if strings.TrimSpace(down) != "" {
			fkRollback = append(fkRollback, down)
		}
	}

	for _, ch := range td.ModifiedConstraints {
		if ch == nil || ch.Old == nil {
			continue
		}
		if drop := g.dropConstraint(table, ch.Old); drop != "" {
			add(drop, g.addConstraint(table, ch.Old))
		}
	}
	for _, rc := range td.RemovedConstraints {
		if rc == nil {
			continue
		}
		if drop := g.dropConstraint(table, rc); drop != "" {
			add(drop, g.addConstraint(table, rc))
		}
	}

	for _, mi := range td.ModifiedIndexes {
		if mi == nil || mi.Old == nil || strings.TrimSpace(mi.Old.Name) == "" {
			continue
		}
		up := fmt.Sprintf("DROP INDEX %s ON %s;", g.QuoteIdentifier(mi.Old.Name), table)
		down := g.createIndex(table, mi.Old)
		add(up, down)
	}
	for _, ri := range td.RemovedIndexes {
		if ri == nil || strings.TrimSpace(ri.Name) == "" {
			continue
		}
		up := fmt.Sprintf("DROP INDEX %s ON %s;", g.QuoteIdentifier(ri.Name), table)
		down := g.createIndex(table, ri)
		add(up, down)
	}

	for _, r := range td.RenamedColumns {
		if r == nil || r.Old == nil || r.New == nil {
			continue
		}
		up := fmt.Sprintf("ALTER TABLE %s CHANGE COLUMN %s %s;", table, g.QuoteIdentifier(r.Old.Name), g.columnDefinition(r.New))
		down := fmt.Sprintf("ALTER TABLE %s CHANGE COLUMN %s %s;", table, g.QuoteIdentifier(r.New.Name), g.columnDefinition(r.Old))
		add(up, down)
	}

	for _, c := range td.AddedColumns {
		if c == nil {
			continue
		}
		up := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s;", table, g.columnDefinition(c))
		down := fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s;", table, g.QuoteIdentifier(c.Name))
		add(up, down)
	}
	for _, ch := range td.ModifiedColumns {
		if ch == nil || ch.New == nil || ch.Old == nil {
			continue
		}
		up := fmt.Sprintf("ALTER TABLE %s MODIFY COLUMN %s;", table, g.columnDefinition(ch.New))
		down := fmt.Sprintf("ALTER TABLE %s MODIFY COLUMN %s;", table, g.columnDefinition(ch.Old))
		add(up, down)
	}
	for _, c := range td.RemovedColumns {
		if c == nil {
			continue
		}
		if opts.IncludeUnsafe {
			up := fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s;", table, g.QuoteIdentifier(c.Name))
			down := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s;", table, g.columnDefinition(c))
			add(up, down)
			continue
		}
		backupName := g.safeBackupName(c.Name)
		backupCol := *c
		backupCol.Name = backupName
		up := fmt.Sprintf("ALTER TABLE %s CHANGE COLUMN %s %s;", table, g.QuoteIdentifier(c.Name), g.columnDefinition(&backupCol))
		down := fmt.Sprintf("ALTER TABLE %s CHANGE COLUMN %s %s;", table, g.QuoteIdentifier(backupName), g.columnDefinition(c))
		add(up, down)
	}

	for _, mo := range td.ModifiedOptions {
		if mo == nil {
			continue
		}
		up := g.alterOption(table, mo)
		if up == "" {
			continue
		}
		down := g.alterOption(table, &diff.TableOptionChange{Name: mo.Name, Old: mo.New, New: mo.Old})
		add(up, down)
	}

	for _, mi := range td.ModifiedIndexes {
		if mi == nil || mi.New == nil {
			continue
		}
		up := g.createIndex(table, mi.New)
		down := ""
		if mi.New != nil && strings.TrimSpace(mi.New.Name) != "" {
			down = fmt.Sprintf("DROP INDEX %s ON %s;", g.QuoteIdentifier(mi.New.Name), table)
		}
		add(up, down)
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
		add(up, down)
	}

	for _, mc := range td.ModifiedConstraints {
		if mc == nil || mc.New == nil {
			continue
		}
		if mc.New.Type == core.ConstraintForeignKey {
			if addStmt := g.addConstraint(table, mc.New); addStmt != "" {
				addFK(addStmt, g.dropConstraint(table, mc.New))
			}
			continue
		}
		if addStmt := g.addConstraint(table, mc.New); addStmt != "" {
			add(addStmt, g.dropConstraint(table, mc.New))
		}
	}
	for _, ac := range td.AddedConstraints {
		if ac == nil {
			continue
		}
		if ac.Type == core.ConstraintForeignKey {
			if addStmt := g.addConstraint(table, ac); addStmt != "" {
				addFK(addStmt, g.dropConstraint(table, ac))
			}
			continue
		}
		if addStmt := g.addConstraint(table, ac); addStmt != "" {
			add(addStmt, g.dropConstraint(table, ac))
		}
	}

	return stmts, rollback, fkAdds, fkRollback
}

func (g *Generator) generateAlterTable(td *diff.TableDiff) ([]string, []string) {
	table := g.QuoteIdentifier(td.Name)
	var stmts []string
	var fkAdds []string

	for _, mc := range td.ModifiedConstraints {
		if mc == nil || mc.Old == nil {
			continue
		}
		if drop := g.dropConstraint(table, mc.Old); drop != "" {
			stmts = append(stmts, drop)
		}
	}
	for _, rc := range td.RemovedConstraints {
		if rc == nil {
			continue
		}
		if drop := g.dropConstraint(table, rc); drop != "" {
			stmts = append(stmts, drop)
		}
	}

	for _, mi := range td.ModifiedIndexes {
		if mi == nil || mi.Old == nil || strings.TrimSpace(mi.Old.Name) == "" {
			continue
		}
		stmts = append(stmts, fmt.Sprintf(
			"DROP INDEX %s ON %s;", g.QuoteIdentifier(mi.Old.Name), table))
	}
	for _, ri := range td.RemovedIndexes {
		if ri == nil || strings.TrimSpace(ri.Name) == "" {
			continue
		}
		stmts = append(stmts, fmt.Sprintf(
			"DROP INDEX %s ON %s;", g.QuoteIdentifier(ri.Name), table))
	}

	for _, ac := range td.AddedColumns {
		if ac == nil {
			continue
		}
		stmts = append(stmts, fmt.Sprintf(
			"ALTER TABLE %s ADD COLUMN %s;", table, g.columnDefinition(ac)))
	}
	for _, mc := range td.ModifiedColumns {
		if mc == nil || mc.New == nil {
			continue
		}
		stmts = append(stmts, fmt.Sprintf(
			"ALTER TABLE %s MODIFY COLUMN %s;", table, g.columnDefinition(mc.New)))
	}
	for _, rc := range td.RemovedColumns {
		if rc == nil {
			continue
		}
		stmts = append(stmts, fmt.Sprintf(
			"ALTER TABLE %s DROP COLUMN %s;", table, g.QuoteIdentifier(rc.Name)))
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

	for _, mc := range td.ModifiedConstraints {
		if mc == nil {
			continue
		}
		if mc.New != nil && mc.New.Type == core.ConstraintForeignKey {
			if add := g.addConstraint(table, mc.New); add != "" {
				fkAdds = append(fkAdds, add)
			}
			continue
		}
		if add := g.addConstraint(table, mc.New); add != "" {
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
