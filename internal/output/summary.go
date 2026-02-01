package output

import (
	"fmt"
	"strings"

	"smf/internal/diff"
	"smf/internal/migration"
)

type summaryFormatter struct{}

// FormatDiff formats a schema diff as a compact summary.
// Example output:
//
//	Tables:    +3, ~2, -0
//	Columns:   +5, ~2, -0
//	Indexes:   +1, ~0, -2
func (summaryFormatter) FormatDiff(d *diff.SchemaDiff) (string, error) {
	if d == nil {
		return "No changes detected.\n", nil
	}

	var sb strings.Builder

	addedTables := len(d.AddedTables)
	removedTables := len(d.RemovedTables)
	modifiedTables := len(d.ModifiedTables)

	addedCols, removedCols, modifiedCols := countColumns(d)
	addedIdx, removedIdx, modifiedIdx := countIndexes(d)
	addedConstr, removedConstr, modifiedConstr := countConstraints(d)

	sb.WriteString("Schema Diff Summary\n")
	sb.WriteString("===================\n\n")

	fmt.Fprintf(&sb, "Tables:      +%d, ~%d, -%d\n", addedTables, modifiedTables, removedTables)
	fmt.Fprintf(&sb, "Columns:     +%d, ~%d, -%d\n", addedCols, modifiedCols, removedCols)
	fmt.Fprintf(&sb, "Indexes:     +%d, ~%d, -%d\n", addedIdx, modifiedIdx, removedIdx)
	fmt.Fprintf(&sb, "Constraints: +%d, ~%d, -%d\n", addedConstr, modifiedConstr, removedConstr)

	if len(d.Warnings) > 0 {
		fmt.Fprintf(&sb, "\nWarnings:    %d\n", len(d.Warnings))
	}

	writeTableDetails(&sb, d, addedTables, removedTables, modifiedTables)

	return sb.String(), nil
}

func countColumns(d *diff.SchemaDiff) (added, removed, modified int) {
	for _, t := range d.AddedTables {
		added += len(t.Columns)
	}
	for _, t := range d.RemovedTables {
		removed += len(t.Columns)
	}
	for _, td := range d.ModifiedTables {
		added += len(td.AddedColumns)
		removed += len(td.RemovedColumns)
		modified += len(td.ModifiedColumns)
	}
	return
}

func countIndexes(d *diff.SchemaDiff) (added, removed, modified int) {
	for _, t := range d.AddedTables {
		added += len(t.Indexes)
	}
	for _, t := range d.RemovedTables {
		removed += len(t.Indexes)
	}
	for _, td := range d.ModifiedTables {
		added += len(td.AddedIndexes)
		removed += len(td.RemovedIndexes)
		modified += len(td.ModifiedIndexes)
	}
	return
}

func countConstraints(d *diff.SchemaDiff) (added, removed, modified int) {
	for _, t := range d.AddedTables {
		added += len(t.Constraints)
	}
	for _, t := range d.RemovedTables {
		removed += len(t.Constraints)
	}
	for _, td := range d.ModifiedTables {
		added += len(td.AddedConstraints)
		removed += len(td.RemovedConstraints)
		modified += len(td.ModifiedConstraints)
	}
	return
}

func writeTableDetails(sb *strings.Builder, d *diff.SchemaDiff, addedTables, removedTables, modifiedTables int) {
	if addedTables == 0 && removedTables == 0 && modifiedTables == 0 {
		return
	}

	sb.WriteString("\nDetails:\n")
	for _, t := range d.AddedTables {
		fmt.Fprintf(sb, "  + %s (new table)\n", t.Name)
	}
	for _, t := range d.RemovedTables {
		fmt.Fprintf(sb, "  - %s (removed table)\n", t.Name)
	}
	for _, td := range d.ModifiedTables {
		changes := countTableChanges(td)
		fmt.Fprintf(sb, "  ~ %s (%s)\n", td.Name, changes)
	}
}

// countTableChanges returns a human-readable summary of changes in a table.
func countTableChanges(td *diff.TableDiff) string {
	var parts []string

	if n := len(td.AddedColumns); n > 0 {
		parts = append(parts, fmt.Sprintf("+%d cols", n))
	}
	if n := len(td.RemovedColumns); n > 0 {
		parts = append(parts, fmt.Sprintf("-%d cols", n))
	}
	if n := len(td.ModifiedColumns); n > 0 {
		parts = append(parts, fmt.Sprintf("~%d cols", n))
	}
	if n := len(td.AddedIndexes); n > 0 {
		parts = append(parts, fmt.Sprintf("+%d idx", n))
	}
	if n := len(td.RemovedIndexes); n > 0 {
		parts = append(parts, fmt.Sprintf("-%d idx", n))
	}
	if n := len(td.AddedConstraints); n > 0 {
		parts = append(parts, fmt.Sprintf("+%d fk", n))
	}
	if n := len(td.RemovedConstraints); n > 0 {
		parts = append(parts, fmt.Sprintf("-%d fk", n))
	}

	if len(parts) == 0 {
		return "options changed"
	}
	return strings.Join(parts, ", ")
}

// FormatMigration formats a migration as a compact summary.
func (summaryFormatter) FormatMigration(m *migration.Migration) (string, error) {
	if m == nil || len(m.Operations) == 0 {
		return "No migration operations.\n", nil
	}

	var sb strings.Builder

	breaking := m.BreakingNotes()
	unresolved := m.UnresolvedNotes()
	notes := m.InfoNotes()
	sql := m.SQLStatements()
	rollback := m.RollbackStatements()

	sb.WriteString("Migration Summary\n")
	sb.WriteString("=================\n\n")

	fmt.Fprintf(&sb, "SQL Statements:      %d\n", len(sql))
	fmt.Fprintf(&sb, "Rollback Statements: %d\n", len(rollback))

	if len(breaking) > 0 {
		fmt.Fprintf(&sb, "\nBreaking Changes: %d\n", len(breaking))
		for _, b := range breaking {
			fmt.Fprintf(&sb, "   - %s\n", b)
		}
	}

	if len(unresolved) > 0 {
		fmt.Fprintf(&sb, "\nUnresolved Issues: %d\n", len(unresolved))
		for _, u := range unresolved {
			fmt.Fprintf(&sb, "   - %s\n", u)
		}
	}

	if len(notes) > 0 {
		fmt.Fprintf(&sb, "\nNotes: %d\n", len(notes))
		for _, n := range notes {
			fmt.Fprintf(&sb, "   - %s\n", n)
		}
	}

	return sb.String(), nil
}
