package mysql

import (
	"fmt"
	"smf/diff"
	"strings"
)

func (g *Generator) rollbackSuggestions(schemaDiff *diff.SchemaDiff) []string {
	if schemaDiff == nil {
		return nil
	}
	var out []string

	for _, t := range schemaDiff.AddedTables {
		if t == nil {
			continue
		}
		out = append(out, g.GenerateDropTable(t))
	}

	for _, t := range schemaDiff.RemovedTables {
		if t == nil {
			continue
		}
		out = append(out, fmt.Sprintf("-- cannot auto-rollback DROP TABLE %s (restore from backup)", g.QuoteIdentifier(t.Name)))
	}

	for _, td := range schemaDiff.ModifiedTables {
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
			out = append(out, fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s;", table, g.columnDefinition(rc)))
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
			if stmt := g.alterOption(table, &diff.TableOptionChange{Name: mo.Name, New: mo.Old}); stmt != "" {
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
