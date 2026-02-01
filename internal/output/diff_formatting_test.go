package output

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"smf/internal/core"
	"smf/internal/diff"
	"smf/internal/parser"
)

func TestFormatDiffText(t *testing.T) {
	oldSQL := `CREATE TABLE users (
		id INT PRIMARY KEY AUTO_INCREMENT,
		name VARCHAR(255) NULL
	);

	CREATE TABLE posts (
		id INT PRIMARY KEY
	);`

	newSQL := `CREATE TABLE users (
		id INT PRIMARY KEY AUTO_INCREMENT,
		name VARCHAR(255) NOT NULL,
		email VARCHAR(255)
	);

	CREATE TABLE comments (
		id INT PRIMARY KEY
	);`

	p := parser.NewSQLParser()
	oldDB, err := p.ParseSchema(oldSQL)
	require.NoError(t, err)
	newDB, err := p.ParseSchema(newSQL)
	require.NoError(t, err)

	d := diff.Diff(oldDB, newDB, diff.DefaultOptions())
	require.NotNil(t, d)

	formatter, err := NewFormatter("sql")
	require.NoError(t, err)

	s, err := formatter.FormatDiff(d)
	require.NoError(t, err)

	assert.Contains(t, s, "Schema differences:")
	assert.Contains(t, s, "Added tables:")
	assert.Contains(t, s, "comments")
	assert.Contains(t, s, "Removed tables:")
	assert.Contains(t, s, "posts")
	assert.Contains(t, s, "Modified tables:")
	assert.Contains(t, s, "users")
	assert.Contains(t, s, "Added columns:")
	assert.Contains(t, s, "email")
	assert.Contains(t, s, "Modified columns:")
	assert.Contains(t, s, "name")
}

func TestFormatDiffTextEmpty(t *testing.T) {
	d := &diff.SchemaDiff{}
	result := formatDiffText(d)
	assert.Equal(t, "No differences detected.", result)
}

func TestFormatDiffTextWithWarnings(t *testing.T) {
	d := &diff.SchemaDiff{
		Warnings: []string{"Warning 1", "Warning 2"},
		AddedTables: []*core.Table{
			{Name: "users"},
		},
	}
	result := formatDiffText(d)
	assert.Contains(t, result, "Warnings:")
	assert.Contains(t, result, "- Warning 1")
	assert.Contains(t, result, "- Warning 2")
}

func TestFormatDiffTextAddedTablesOnly(t *testing.T) {
	d := &diff.SchemaDiff{
		AddedTables: []*core.Table{
			{Name: "users"},
			{Name: "posts"},
		},
	}
	result := formatDiffText(d)
	assert.Contains(t, result, "Added tables:")
	assert.Contains(t, result, "- users")
	assert.Contains(t, result, "- posts")
	assert.NotContains(t, result, "Removed tables:")
	assert.NotContains(t, result, "Modified tables:")
}

func TestFormatDiffTextRemovedTablesOnly(t *testing.T) {
	d := &diff.SchemaDiff{
		RemovedTables: []*core.Table{
			{Name: "old_table"},
		},
	}
	result := formatDiffText(d)
	assert.Contains(t, result, "Removed tables:")
	assert.Contains(t, result, "- old_table")
	assert.NotContains(t, result, "Added tables:")
}

func TestWriteTableDiffTextAddedColumns(t *testing.T) {
	td := &diff.TableDiff{
		Name: "users",
		AddedColumns: []*core.Column{
			{Name: "email", TypeRaw: "VARCHAR(255)"},
			{Name: "age", TypeRaw: "INT"},
		},
	}
	var sb strings.Builder
	writeTableDiffText(&sb, td)
	result := sb.String()
	assert.Contains(t, result, "- users")
	assert.Contains(t, result, "Added columns:")
	assert.Contains(t, result, "- email: VARCHAR(255)")
	assert.Contains(t, result, "- age: INT")
}

func TestWriteTableDiffTextRemovedColumns(t *testing.T) {
	td := &diff.TableDiff{
		Name: "users",
		RemovedColumns: []*core.Column{
			{Name: "old_field", TypeRaw: "TEXT"},
		},
	}
	var sb strings.Builder
	writeTableDiffText(&sb, td)
	result := sb.String()
	assert.Contains(t, result, "Removed columns:")
	assert.Contains(t, result, "- old_field: TEXT")
}

func TestWriteTableDiffTextModifiedColumns(t *testing.T) {
	td := &diff.TableDiff{
		Name: "users",
		ModifiedColumns: []*diff.ColumnChange{
			{
				Name: "name",
				Changes: []*diff.FieldChange{
					{Field: "Nullable", Old: "true", New: "false"},
				},
			},
		},
	}
	var sb strings.Builder
	writeTableDiffText(&sb, td)
	result := sb.String()
	assert.Contains(t, result, "Modified columns:")
	assert.Contains(t, result, "- name:")
	assert.Contains(t, result, "- Nullable: \"true\" -> \"false\"")
}

func TestWriteTableDiffTextAddedIndexes(t *testing.T) {
	td := &diff.TableDiff{
		Name: "users",
		AddedIndexes: []*core.Index{
			{Name: "idx_email", Columns: []core.IndexColumn{{Name: "email"}}},
		},
	}
	var sb strings.Builder
	writeTableDiffText(&sb, td)
	result := sb.String()
	assert.Contains(t, result, "Added indexes:")
	assert.Contains(t, result, "- idx_email")
}

func TestWriteTableDiffTextRemovedIndexes(t *testing.T) {
	td := &diff.TableDiff{
		Name: "users",
		RemovedIndexes: []*core.Index{
			{Name: "idx_old", Columns: []core.IndexColumn{{Name: "old_field"}}},
		},
	}
	var sb strings.Builder
	writeTableDiffText(&sb, td)
	result := sb.String()
	assert.Contains(t, result, "Removed indexes:")
	assert.Contains(t, result, "- idx_old")
}

func TestWriteTableDiffTextModifiedIndexes(t *testing.T) {
	td := &diff.TableDiff{
		Name: "users",
		ModifiedIndexes: []*diff.IndexChange{
			{
				Name: "idx_name",
				Changes: []*diff.FieldChange{
					{Field: "Unique", Old: "false", New: "true"},
				},
			},
		},
	}
	var sb strings.Builder
	writeTableDiffText(&sb, td)
	result := sb.String()
	assert.Contains(t, result, "Modified indexes:")
	assert.Contains(t, result, "- idx_name:")
	assert.Contains(t, result, "- Unique: \"false\" -> \"true\"")
}

func TestWriteTableDiffTextAddedConstraints(t *testing.T) {
	td := &diff.TableDiff{
		Name: "users",
		AddedConstraints: []*core.Constraint{
			{Name: "fk_user_post", Type: core.ConstraintForeignKey},
		},
	}
	var sb strings.Builder
	writeTableDiffText(&sb, td)
	result := sb.String()
	assert.Contains(t, result, "Added constraints:")
	assert.Contains(t, result, "- fk_user_post (FOREIGN KEY)")
}

func TestWriteTableDiffTextRemovedConstraints(t *testing.T) {
	td := &diff.TableDiff{
		Name: "users",
		RemovedConstraints: []*core.Constraint{
			{Name: "fk_old", Type: core.ConstraintForeignKey},
		},
	}
	var sb strings.Builder
	writeTableDiffText(&sb, td)
	result := sb.String()
	assert.Contains(t, result, "Removed constraints:")
	assert.Contains(t, result, "- fk_old (FOREIGN KEY)")
}

func TestWriteTableDiffTextModifiedConstraints(t *testing.T) {
	oldConstraint := &core.Constraint{
		Name: "fk_test",
		Type: core.ConstraintForeignKey,
	}
	newConstraint := &core.Constraint{
		Name: "fk_test",
		Type: core.ConstraintForeignKey,
	}
	td := &diff.TableDiff{
		Name: "users",
		ModifiedConstraints: []*diff.ConstraintChange{
			{
				Name: "fk_test",
				Old:  oldConstraint,
				New:  newConstraint,
				Changes: []*diff.FieldChange{
					{Field: "OnDelete", Old: "CASCADE", New: "SET NULL"},
				},
			},
		},
	}
	var sb strings.Builder
	writeTableDiffText(&sb, td)
	result := sb.String()
	assert.Contains(t, result, "Modified constraints:")
	assert.Contains(t, result, "- fk_test:")
	assert.Contains(t, result, "- OnDelete: \"CASCADE\" -> \"SET NULL\"")
}

func TestWriteTableDiffTextModifiedConstraintsUnnamed(t *testing.T) {
	td := &diff.TableDiff{
		Name: "users",
		ModifiedConstraints: []*diff.ConstraintChange{
			{
				Name: "",
				New: &core.Constraint{
					Type: core.ConstraintForeignKey,
				},
				Changes: []*diff.FieldChange{
					{Field: "OnDelete", Old: "CASCADE", New: "SET NULL"},
				},
			},
		},
	}
	var sb strings.Builder
	writeTableDiffText(&sb, td)
	result := sb.String()
	assert.Contains(t, result, "Modified constraints:")
	assert.Contains(t, result, "- FOREIGN KEY:")
}

func TestWriteTableDiffTextModifiedConstraintsNil(t *testing.T) {
	td := &diff.TableDiff{
		Name: "users",
		ModifiedConstraints: []*diff.ConstraintChange{
			nil,
			{
				Name: "fk_valid",
				Changes: []*diff.FieldChange{
					{Field: "Test", Old: "a", New: "b"},
				},
			},
		},
	}
	var sb strings.Builder
	writeTableDiffText(&sb, td)
	result := sb.String()
	assert.Contains(t, result, "Modified constraints:")
	assert.Contains(t, result, "- fk_valid:")
	assert.NotContains(t, result, "(unnamed)")
}

func TestWriteTableDiffTextModifiedOptions(t *testing.T) {
	td := &diff.TableDiff{
		Name: "users",
		ModifiedOptions: []*diff.TableOptionChange{
			{Name: "ENGINE", Old: "InnoDB", New: "MyISAM"},
			{Name: "CHARSET", Old: "utf8", New: "utf8mb4"},
		},
	}
	var sb strings.Builder
	writeTableDiffText(&sb, td)
	result := sb.String()
	assert.Contains(t, result, "Options changed:")
	assert.Contains(t, result, "- ENGINE: \"InnoDB\" -> \"MyISAM\"")
	assert.Contains(t, result, "- CHARSET: \"utf8\" -> \"utf8mb4\"")
}

func TestWriteTableDiffTextWithWarnings(t *testing.T) {
	td := &diff.TableDiff{
		Name:     "users",
		Warnings: []string{"Potential data loss", "Manual review needed"},
	}
	var sb strings.Builder
	writeTableDiffText(&sb, td)
	result := sb.String()
	assert.Contains(t, result, "Warnings:")
	assert.Contains(t, result, "- Potential data loss")
	assert.Contains(t, result, "- Manual review needed")
}

func TestWriteTableDiffTextAllSections(t *testing.T) {
	td := &diff.TableDiff{
		Name:     "users",
		Warnings: []string{"Warning message"},
		ModifiedOptions: []*diff.TableOptionChange{
			{Name: "ENGINE", Old: "InnoDB", New: "MyISAM"},
		},
		AddedColumns: []*core.Column{
			{Name: "email", TypeRaw: "VARCHAR(255)"},
		},
		RemovedColumns: []*core.Column{
			{Name: "old_col", TypeRaw: "TEXT"},
		},
		ModifiedColumns: []*diff.ColumnChange{
			{
				Name: "name",
				Changes: []*diff.FieldChange{
					{Field: "Type", Old: "VARCHAR(100)", New: "VARCHAR(255)"},
				},
			},
		},
		AddedIndexes: []*core.Index{
			{Name: "idx_email"},
		},
		RemovedIndexes: []*core.Index{
			{Name: "idx_old"},
		},
		ModifiedIndexes: []*diff.IndexChange{
			{Name: "idx_name"},
		},
		AddedConstraints: []*core.Constraint{
			{Name: "fk_new", Type: core.ConstraintForeignKey},
		},
		RemovedConstraints: []*core.Constraint{
			{Name: "fk_old", Type: core.ConstraintForeignKey},
		},
		ModifiedConstraints: []*diff.ConstraintChange{
			{Name: "fk_test"},
		},
	}
	var sb strings.Builder
	writeTableDiffText(&sb, td)
	result := sb.String()
	assert.Contains(t, result, "- users")
	assert.Contains(t, result, "Warnings:")
	assert.Contains(t, result, "Options changed:")
	assert.Contains(t, result, "Added columns:")
	assert.Contains(t, result, "Removed columns:")
	assert.Contains(t, result, "Modified columns:")
	assert.Contains(t, result, "Added indexes:")
	assert.Contains(t, result, "Removed indexes:")
	assert.Contains(t, result, "Modified indexes:")
	assert.Contains(t, result, "Added constraints:")
	assert.Contains(t, result, "Removed constraints:")
	assert.Contains(t, result, "Modified constraints:")
}

func TestSQLFormatterFormatDiffNil(t *testing.T) {
	sf := sqlFormatter{}
	result, err := sf.FormatDiff(nil)
	assert.NoError(t, err)
	assert.Equal(t, "", result)
}

func TestSQLFormatterFormatDiffEmpty(t *testing.T) {
	sf := sqlFormatter{}
	d := &diff.SchemaDiff{}
	result, err := sf.FormatDiff(d)
	assert.NoError(t, err)
	assert.Equal(t, "No differences detected.", result)
}
