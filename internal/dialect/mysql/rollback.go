package mysql

import (
	"fmt"
	"strings"

	"smf/internal/core"
	"smf/internal/diff"
)

func (g *Generator) rollbackSuggestions(schemaDiff *diff.SchemaDiff) []string {
	var out []string

	out = append(out, g.rollbackAddedTables(schemaDiff.AddedTables)...)
	out = append(out, g.rollbackRemovedTables(schemaDiff.RemovedTables)...)
	out = append(out, g.rollbackModifiedTables(schemaDiff.ModifiedTables)...)

	return g.cleanStatements(out)
}

func (g *Generator) rollbackAddedTables(tables []*core.Table) []string {
	var out []string
	for _, t := range tables {
		if t == nil {
			continue
		}
		out = append(out, g.GenerateDropTable(t))
	}
	return out
}

func (g *Generator) rollbackRemovedTables(tables []*core.Table) []string {
	var out []string
	for _, t := range tables {
		if t == nil {
			continue
		}
		out = append(out, fmt.Sprintf("-- cannot auto-rollback DROP TABLE %s (restore from backup)", g.QuoteIdentifier(t.Name)))
	}
	return out
}

func (g *Generator) rollbackModifiedTables(tableDiffs []*diff.TableDiff) []string {
	var out []string
	for _, td := range tableDiffs {
		if td == nil {
			continue
		}
		table := g.QuoteIdentifier(td.Name)

		out = append(out, g.rollbackColumns(table, td)...)
		out = append(out, g.rollbackConstraints(table, td)...)
		out = append(out, g.rollbackIndexes(table, td)...)
		out = append(out, g.rollbackOptions(table, td)...)
	}
	return out
}

func (g *Generator) rollbackColumns(table string, td *diff.TableDiff) []string {
	var out []string
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
		out = append(out, fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s;", table, g.columnDefinition(rc)))
	}
	for _, mc := range td.ModifiedColumns {
		if mc == nil || mc.Old == nil {
			continue
		}
		out = append(out, fmt.Sprintf("ALTER TABLE %s MODIFY COLUMN %s;", table, g.columnDefinition(mc.Old)))
	}
	return out
}

func (g *Generator) rollbackConstraints(table string, td *diff.TableDiff) []string {
	var out []string
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
	return out
}

func (g *Generator) rollbackIndexes(table string, td *diff.TableDiff) []string {
	var out []string
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
	return out
}

func (g *Generator) rollbackOptions(table string, td *diff.TableDiff) []string {
	var out []string
	for _, mo := range td.ModifiedOptions {
		if mo == nil {
			continue
		}
		if stmt := g.alterOption(table, &diff.TableOptionChange{Name: mo.Name, New: mo.Old}); stmt != "" {
			out = append(out, stmt)
		}
	}
	return out
}

func (g *Generator) cleanStatements(stmts []string) []string {
	var cleaned []string
	for _, s := range stmts {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		cleaned = append(cleaned, s)
	}
	return cleaned
}
