package output

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"smf/internal/core"
	"smf/internal/diff"
	"smf/internal/migration"
)

func TestSummaryFormatterFormatDiffNil(t *testing.T) {
	sf := summaryFormatter{}
	result, err := sf.FormatDiff(nil)
	require.NoError(t, err)
	assert.Equal(t, "No changes detected.\n", result)
}

func TestSummaryFormatterFormatDiffEmpty(t *testing.T) {
	sf := summaryFormatter{}
	d := &diff.SchemaDiff{}
	result, err := sf.FormatDiff(d)
	require.NoError(t, err)
	assert.Contains(t, result, "Schema Diff Summary")
	assert.Contains(t, result, "Tables:      +0, ~0, -0")
	assert.Contains(t, result, "Columns:     +0, ~0, -0")
	assert.Contains(t, result, "Indexes:     +0, ~0, -0")
	assert.Contains(t, result, "Constraints: +0, ~0, -0")
}

func TestSummaryFormatterFormatDiffAddedTables(t *testing.T) {
	sf := summaryFormatter{}
	d := &diff.SchemaDiff{
		AddedTables: []*core.Table{
			{
				Name: "users",
				Columns: []*core.Column{
					{Name: "id", TypeRaw: "INT"},
					{Name: "name", TypeRaw: "VARCHAR(255)"},
				},
				Indexes: []*core.Index{
					{Name: "idx_name"},
				},
				Constraints: []*core.Constraint{
					{Name: "fk_user", Type: core.ConstraintForeignKey},
				},
			},
			{
				Name: "posts",
				Columns: []*core.Column{
					{Name: "id", TypeRaw: "INT"},
				},
			},
		},
	}
	result, err := sf.FormatDiff(d)
	require.NoError(t, err)
	assert.Contains(t, result, "Tables:      +2, ~0, -0")
	assert.Contains(t, result, "Columns:     +3, ~0, -0")
	assert.Contains(t, result, "Indexes:     +1, ~0, -0")
	assert.Contains(t, result, "Constraints: +1, ~0, -0")
	assert.Contains(t, result, "+ users (new table)")
	assert.Contains(t, result, "+ posts (new table)")
}

func TestSummaryFormatterFormatDiffRemovedTables(t *testing.T) {
	sf := summaryFormatter{}
	d := &diff.SchemaDiff{
		RemovedTables: []*core.Table{
			{
				Name: "old_table",
				Columns: []*core.Column{
					{Name: "id", TypeRaw: "INT"},
				},
				Indexes: []*core.Index{
					{Name: "idx_id"},
				},
				Constraints: []*core.Constraint{
					{Name: "fk_old", Type: core.ConstraintForeignKey},
				},
			},
		},
	}
	result, err := sf.FormatDiff(d)
	require.NoError(t, err)
	assert.Contains(t, result, "Tables:      +0, ~0, -1")
	assert.Contains(t, result, "Columns:     +0, ~0, -1")
	assert.Contains(t, result, "Indexes:     +0, ~0, -1")
	assert.Contains(t, result, "Constraints: +0, ~0, -1")
	assert.Contains(t, result, "- old_table (removed table)")
}

func TestSummaryFormatterFormatDiffModifiedTables(t *testing.T) {
	sf := summaryFormatter{}
	d := &diff.SchemaDiff{
		ModifiedTables: []*diff.TableDiff{
			{
				Name: "users",
				AddedColumns: []*core.Column{
					{Name: "email", TypeRaw: "VARCHAR(255)"},
				},
				RemovedColumns: []*core.Column{
					{Name: "old_field", TypeRaw: "TEXT"},
				},
				ModifiedColumns: []*diff.ColumnChange{
					{Name: "name"},
				},
				AddedIndexes: []*core.Index{
					{Name: "idx_email"},
				},
				RemovedIndexes: []*core.Index{
					{Name: "idx_old"},
				},
				ModifiedIndexes: []*diff.IndexChange{
					{Name: "idx_primary"},
				},
				AddedConstraints: []*core.Constraint{
					{Name: "fk_new", Type: core.ConstraintForeignKey},
				},
				RemovedConstraints: []*core.Constraint{
					{Name: "fk_old", Type: core.ConstraintForeignKey},
				},
			},
		},
	}
	result, err := sf.FormatDiff(d)
	require.NoError(t, err)
	assert.Contains(t, result, "Tables:      +0, ~1, -0")
	assert.Contains(t, result, "Columns:     +1, ~1, -1")
	assert.Contains(t, result, "Indexes:     +1, ~1, -1")
	assert.Contains(t, result, "Constraints: +1, ~0, -1")
	assert.Contains(t, result, "~ users (+1 cols, -1 cols, ~1 cols, +1 idx, -1 idx, +1 fk, -1 fk)")
}

func TestSummaryFormatterFormatDiffWithWarnings(t *testing.T) {
	sf := summaryFormatter{}
	d := &diff.SchemaDiff{
		Warnings: []string{"Warning 1", "Warning 2", "Warning 3"},
	}
	result, err := sf.FormatDiff(d)
	require.NoError(t, err)
	assert.Contains(t, result, "Warnings:    3")
}

func TestSummaryFormatterFormatDiffComplexScenario(t *testing.T) {
	sf := summaryFormatter{}
	d := &diff.SchemaDiff{
		AddedTables: []*core.Table{
			{Name: "new_table"},
		},
		RemovedTables: []*core.Table{
			{Name: "removed_table"},
		},
		ModifiedTables: []*diff.TableDiff{
			{
				Name: "table1",
				AddedColumns: []*core.Column{
					{Name: "col1"},
					{Name: "col2"},
				},
			},
			{
				Name: "table2",
				RemovedIndexes: []*core.Index{
					{Name: "idx1"},
				},
			},
		},
		Warnings: []string{"Some warning"},
	}
	result, err := sf.FormatDiff(d)
	require.NoError(t, err)
	assert.Contains(t, result, "Tables:      +1, ~2, -1")
	assert.Contains(t, result, "Columns:     +2, ~0, -0")
	assert.Contains(t, result, "Warnings:    1")
	assert.Contains(t, result, "+ new_table (new table)")
	assert.Contains(t, result, "- removed_table (removed table)")
	assert.Contains(t, result, "~ table1 (+2 cols)")
	assert.Contains(t, result, "~ table2 (-1 idx)")
}

func TestCountTableChangesAllChangeTypes(t *testing.T) {
	td := &diff.TableDiff{
		AddedColumns:       []*core.Column{{Name: "c1"}, {Name: "c2"}},
		RemovedColumns:     []*core.Column{{Name: "c3"}},
		ModifiedColumns:    []*diff.ColumnChange{{Name: "c4"}},
		AddedIndexes:       []*core.Index{{Name: "i1"}},
		RemovedIndexes:     []*core.Index{{Name: "i2"}, {Name: "i3"}},
		AddedConstraints:   []*core.Constraint{{Name: "fk1"}},
		RemovedConstraints: []*core.Constraint{{Name: "fk2"}},
	}
	result := countTableChanges(td)
	assert.Equal(t, "+2 cols, -1 cols, ~1 cols, +1 idx, -2 idx, +1 fk, -1 fk", result)
}

func TestCountTableChangesNoChanges(t *testing.T) {
	td := &diff.TableDiff{}
	result := countTableChanges(td)
	assert.Equal(t, "options changed", result)
}

func TestCountTableChangesOnlyAddedColumns(t *testing.T) {
	td := &diff.TableDiff{
		AddedColumns: []*core.Column{{Name: "c1"}},
	}
	result := countTableChanges(td)
	assert.Equal(t, "+1 cols", result)
}

func TestCountTableChangesOnlyRemovedIndexes(t *testing.T) {
	td := &diff.TableDiff{
		RemovedIndexes: []*core.Index{{Name: "i1"}, {Name: "i2"}},
	}
	result := countTableChanges(td)
	assert.Equal(t, "-2 idx", result)
}

func TestCountTableChangesMultipleConstraints(t *testing.T) {
	td := &diff.TableDiff{
		AddedConstraints:   []*core.Constraint{{Name: "fk1"}, {Name: "fk2"}, {Name: "fk3"}},
		RemovedConstraints: []*core.Constraint{{Name: "fk4"}},
	}
	result := countTableChanges(td)
	assert.Equal(t, "+3 fk, -1 fk", result)
}

func TestSummaryFormatterFormatMigrationNil(t *testing.T) {
	sf := summaryFormatter{}
	result, err := sf.FormatMigration(nil)
	require.NoError(t, err)
	assert.Equal(t, "No migration operations.\n", result)
}

func TestSummaryFormatterFormatMigrationEmpty(t *testing.T) {
	sf := summaryFormatter{}
	m := &migration.Migration{
		Operations: []core.Operation{},
	}
	result, err := sf.FormatMigration(m)
	require.NoError(t, err)
	assert.Equal(t, "No migration operations.\n", result)
}

func TestSummaryFormatterFormatMigrationBasic(t *testing.T) {
	sf := summaryFormatter{}
	m := &migration.Migration{}
	m.AddStatement("CREATE TABLE users (id INT)")
	m.AddRollbackStatement("DROP TABLE users")
	result, err := sf.FormatMigration(m)
	require.NoError(t, err)
	assert.Contains(t, result, "Migration Summary")
	assert.Contains(t, result, "SQL Statements:      1")
	assert.Contains(t, result, "Rollback Statements: 1")
}

func TestSummaryFormatterFormatMigrationWithBreaking(t *testing.T) {
	sf := summaryFormatter{}
	m := &migration.Migration{}
	m.AddStatement("ALTER TABLE users DROP COLUMN email")
	m.AddBreaking("Dropping column email will lose data")
	m.AddBreaking("Another breaking change")
	result, err := sf.FormatMigration(m)
	require.NoError(t, err)
	assert.Contains(t, result, "Breaking Changes: 2")
	assert.Contains(t, result, "- Dropping column email will lose data")
	assert.Contains(t, result, "- Another breaking change")
}

func TestSummaryFormatterFormatMigrationWithUnresolved(t *testing.T) {
	sf := summaryFormatter{}
	m := &migration.Migration{}
	m.AddUnresolved("Cannot determine column rename")
	m.AddUnresolved("Manual review required")
	result, err := sf.FormatMigration(m)
	require.NoError(t, err)
	assert.Contains(t, result, "Unresolved Issues: 2")
	assert.Contains(t, result, "- Cannot determine column rename")
	assert.Contains(t, result, "- Manual review required")
}

func TestSummaryFormatterFormatMigrationWithNotes(t *testing.T) {
	sf := summaryFormatter{}
	m := &migration.Migration{}
	m.AddNote("This is an informational note")
	m.AddNote("Another note")
	m.AddNote("Third note")
	result, err := sf.FormatMigration(m)
	require.NoError(t, err)
	assert.Contains(t, result, "Notes: 3")
	assert.Contains(t, result, "- This is an informational note")
	assert.Contains(t, result, "- Another note")
	assert.Contains(t, result, "- Third note")
}

func TestSummaryFormatterFormatMigrationComplete(t *testing.T) {
	sf := summaryFormatter{}
	m := &migration.Migration{}
	m.AddStatementWithRollback("CREATE TABLE users (id INT)", "DROP TABLE users")
	m.AddStatementWithRollback("CREATE TABLE posts (id INT)", "DROP TABLE posts")
	m.AddBreaking("Breaking change warning")
	m.AddUnresolved("Unresolved issue")
	m.AddNote("Informational note")
	result, err := sf.FormatMigration(m)
	require.NoError(t, err)
	assert.Contains(t, result, "Migration Summary")
	assert.Contains(t, result, "SQL Statements:      2")
	assert.Contains(t, result, "Rollback Statements: 2")
	assert.Contains(t, result, "Breaking Changes: 1")
	assert.Contains(t, result, "- Breaking change warning")
	assert.Contains(t, result, "Unresolved Issues: 1")
	assert.Contains(t, result, "- Unresolved issue")
	assert.Contains(t, result, "Notes: 1")
	assert.Contains(t, result, "- Informational note")
}

func TestSummaryFormatterFormatMigrationMultipleStatements(t *testing.T) {
	sf := summaryFormatter{}
	m := &migration.Migration{}
	for range 10 {
		m.AddStatement("ALTER TABLE t ADD COLUMN c INT")
	}
	for range 5 {
		m.AddRollbackStatement("ALTER TABLE t DROP COLUMN c")
	}
	result, err := sf.FormatMigration(m)
	require.NoError(t, err)
	assert.Contains(t, result, "SQL Statements:      10")
	assert.Contains(t, result, "Rollback Statements: 5")
}

func TestSummaryFormatterFormatMigrationNoSQL(t *testing.T) {
	sf := summaryFormatter{}
	m := &migration.Migration{}
	m.AddNote("Just a note")
	result, err := sf.FormatMigration(m)
	require.NoError(t, err)
	assert.Contains(t, result, "SQL Statements:      0")
	assert.Contains(t, result, "Rollback Statements: 0")
	assert.Contains(t, result, "Notes: 1")
}
