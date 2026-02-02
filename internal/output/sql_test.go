package output

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"smf/internal/core"
	"smf/internal/diff"
	"smf/internal/migration"
)

func TestSQLFormatterFormatMigrationBasicFunctionality(t *testing.T) {
	m := &migration.Migration{}
	m.Operations = append(m.Operations, core.Operation{
		Kind: core.OperationSQL,
		SQL:  "CREATE TABLE t1 (id INT)",
		Risk: core.RiskCritical,
	})
	m.Operations = append(m.Operations, core.Operation{
		Kind: core.OperationBreaking,
		SQL:  "manual review needed",
	})
	m.Operations = append(m.Operations, core.Operation{
		Kind:        core.OperationSQL,
		SQL:         "SELECT 1",
		RollbackSQL: "DROP TABLE t1",
	})

	f := sqlFormatter{}
	out, err := f.FormatMigration(m)
	assert.NoError(t, err)
	assert.Contains(t, out, "CREATE TABLE t1 (id INT)")
	assert.Contains(t, out, "-- [CRITICAL]")
	assert.Contains(t, out, "-- ROLLBACK SQL")
	assert.Contains(t, out, "DROP TABLE t1")
}

func TestSQLFormatterFormatMigrationEmptyFunctionality(t *testing.T) {
	m := &migration.Migration{}
	f := sqlFormatter{}
	out, err := f.FormatMigration(m)
	assert.NoError(t, err)
	assert.Contains(t, out, "-- No SQL statements generated.")

	m.Operations = append(m.Operations, core.Operation{
		Kind:        core.OperationSQL,
		RollbackSQL: "DROP TABLE t1",
	})
	out, err = f.FormatMigration(m)
	assert.NoError(t, err)
	assert.Contains(t, out, "-- ROLLBACK SQL (run separately if needed)")
}

func TestFormatRollbackSQLFunctionality(t *testing.T) {
	m := &migration.Migration{}
	m.Operations = append(m.Operations, core.Operation{
		Kind:        core.OperationSQL,
		RollbackSQL: "STMT 1",
	})
	m.Operations = append(m.Operations, core.Operation{
		Kind:        core.OperationSQL,
		RollbackSQL: "STMT 2",
	})

	out := FormatRollbackSQL(m)
	assert.Contains(t, out, "STMT 2;") // Rollback is reversed
	assert.Contains(t, out, "STMT 1;")
}

func TestSQLFormatterFormatDiffNilFunctionality(t *testing.T) {
	f := sqlFormatter{}
	out, err := f.FormatDiff(nil)
	assert.NoError(t, err)
	assert.Empty(t, out)

	out, err = f.FormatMigration(nil)
	assert.NoError(t, err)
	assert.Empty(t, out)

	assert.Empty(t, FormatRollbackSQL(nil))
}

func TestWriteRiskCommentDetailsFunctionality(t *testing.T) {
	var sb strings.Builder
	writeRiskComment(&sb, core.Operation{Risk: core.RiskCritical, RequiresLock: true})
	assert.Contains(t, sb.String(), "-- [CRITICAL] (may acquire locks)")
}

func TestFormatEmptyMigrationDetailsFunctionality(t *testing.T) {
	var sb strings.Builder
	formatEmptyMigration(&sb, []string{"DROP TABLE t"})
	assert.Contains(t, sb.String(), "No SQL statements generated")
	assert.Contains(t, sb.String(), "ROLLBACK SQL")
}

func TestSaveMigrationToFileFunctionality(t *testing.T) {
	m := &migration.Migration{}
	m.Operations = append(m.Operations, core.Operation{Kind: core.OperationSQL, SQL: "SELECT 1"})

	tmpDir := t.TempDir()
	migrationPath := filepath.Join(tmpDir, "/migration.sql")
	err := SaveMigrationToFile(m, migrationPath)
	assert.NoError(t, err)

	rollbackPath := filepath.Join(tmpDir, "/rollback.sql")
	err = SaveRollbackToFile(m, rollbackPath)
	assert.NoError(t, err)
}

func TestSaveRollbackToFileFunctionality(t *testing.T) {
	m := &migration.Migration{}
	m.Operations = append(m.Operations, core.Operation{Kind: core.OperationSQL, SQL: "SELECT 1"})

	tmpDir := t.TempDir()
	migrationPath := filepath.Join(tmpDir, "/migration.sql")
	err := SaveMigrationToFile(m, migrationPath)
	assert.NoError(t, err)

	rollbackPath := filepath.Join(tmpDir, "/rollback.sql")
	err = SaveRollbackToFile(m, rollbackPath)
	assert.NoError(t, err)
}

func TestJSONFormatterOutput(t *testing.T) {
	f, err := NewFormatter("json")
	assert.NoError(t, err)
	d := &diff.SchemaDiff{AddedTables: []*core.Table{{Name: "t1"}}}
	out, err := f.FormatDiff(d)
	assert.NoError(t, err)
	assert.Contains(t, out, "t1")

	m := &migration.Migration{Operations: []core.Operation{{Kind: core.OperationSQL, SQL: "SELECT 1"}}}
	out, err = f.FormatMigration(m)
	assert.NoError(t, err)
	assert.Contains(t, out, "SELECT 1")
}

func TestDiffTextWarningsDetailsFunctionality(t *testing.T) {
	d := &diff.SchemaDiff{
		Warnings: []string{"W1", "W2"},
		ModifiedTables: []*diff.TableDiff{
			{Name: "t1", Warnings: []string{"WT1"}},
		},
	}
	out := formatDiffText(d)
	assert.Contains(t, out, "Warnings:")
	assert.Contains(t, out, "  - W1")
	assert.Contains(t, out, "      - WT1")
}

func TestGetConstraintNameDetailsFunctionality(t *testing.T) {
	assert.Equal(t, "PRIMARY KEY", getConstraintName(&diff.ConstraintChange{Name: "PRIMARY KEY", New: &core.Constraint{Type: core.ConstraintPrimaryKey}}))
	assert.Equal(t, "fk1", getConstraintName(&diff.ConstraintChange{Name: "fk1", New: &core.Constraint{Type: core.ConstraintForeignKey, Name: "fk1"}}))
}
