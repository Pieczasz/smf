package output

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"smf/internal/core"
	"smf/internal/migration"
)

func TestMigrationStringMultiLineNotesAreCommented(t *testing.T) {
	m := &migration.Migration{}
	m.AddNote("line1\nline2")
	m.AddRollbackStatement("ALTER TABLE t ADD COLUMN c INT")

	out, err := sqlFormatter{}.FormatMigration(m)
	assert.NoError(t, err)
	assert.Contains(t, out, "-- NOTES")
	assert.Contains(t, out, "-- - line1")
	assert.Contains(t, out, "-- - line2")
	assert.NotContains(t, out, "\nline2\n")
	assert.Contains(t, out, "-- ROLLBACK SQL")
	assert.Contains(t, out, "-- ALTER TABLE t ADD COLUMN c INT;")

	rb := FormatRollbackSQL(m)
	assert.Contains(t, rb, "-- smf rollback")
	assert.Contains(t, rb, "ALTER TABLE t ADD COLUMN c INT;")
}

func TestMigrationSaveRollbackToFileWritesRollbackSQL(t *testing.T) {
	m := &migration.Migration{}
	m.AddRollbackStatement("ALTER TABLE t ADD COLUMN c INT")

	f, err := os.CreateTemp(t.TempDir(), "smf-rollback-*.sql")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	name := f.Name()
	_ = f.Close()

	assert.NoError(t, SaveRollbackToFile(m, name))
	b, err := os.ReadFile(name)
	assert.NoError(t, err)
	out := string(b)
	assert.Contains(t, out, "-- smf rollback")
	assert.Contains(t, out, "ALTER TABLE t ADD COLUMN c INT;")
}

func TestSaveMigrationToFileCreatesFile(t *testing.T) {
	m := &migration.Migration{}
	m.AddStatementWithRollback("CREATE TABLE users (id INT)", "DROP TABLE users")

	f, err := os.CreateTemp(t.TempDir(), "smf-migration-*.sql")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	name := f.Name()
	_ = f.Close()

	assert.NoError(t, SaveMigrationToFile(m, name))
	b, err := os.ReadFile(name)
	assert.NoError(t, err)
	out := string(b)
	assert.Contains(t, out, "-- smf migration")
	assert.Contains(t, out, "CREATE TABLE users (id INT);")
	assert.Contains(t, out, "-- ROLLBACK SQL")
}

func TestSaveMigrationToFileInvalidPath(t *testing.T) {
	m := &migration.Migration{}
	m.AddStatement("CREATE TABLE users (id INT)")

	err := SaveMigrationToFile(m, "/nonexistent/directory/file.sql")
	assert.Error(t, err)
}

func TestSaveMigrationToFileEmptyMigration(t *testing.T) {
	m := &migration.Migration{}

	f, err := os.CreateTemp(t.TempDir(), "smf-empty-*.sql")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	name := f.Name()
	_ = f.Close()

	assert.NoError(t, SaveMigrationToFile(m, name))
	b, err := os.ReadFile(name)
	assert.NoError(t, err)
	out := string(b)
	assert.Contains(t, out, "-- smf migration")
	assert.Contains(t, out, "-- No SQL statements generated")
}

func TestSaveRollbackToFileInvalidPath(t *testing.T) {
	m := &migration.Migration{}
	m.AddRollbackStatement("DROP TABLE users")

	err := SaveRollbackToFile(m, "/nonexistent/directory/rollback.sql")
	assert.Error(t, err)
}

func TestSaveRollbackToFileEmptyRollback(t *testing.T) {
	m := &migration.Migration{}

	f, err := os.CreateTemp(t.TempDir(), "smf-empty-rollback-*.sql")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	name := f.Name()
	_ = f.Close()

	assert.NoError(t, SaveRollbackToFile(m, name))
	b, err := os.ReadFile(name)
	assert.NoError(t, err)
	out := string(b)
	assert.Contains(t, out, "-- smf rollback")
	assert.Contains(t, out, "-- No rollback statements generated")
}

func TestSQLFormatterFormatMigrationNil(t *testing.T) {
	sf := sqlFormatter{}
	result, err := sf.FormatMigration(nil)
	assert.NoError(t, err)
	assert.Equal(t, "", result)
}

func TestSQLFormatterFormatMigrationNoOperations(t *testing.T) {
	sf := sqlFormatter{}
	m := &migration.Migration{}
	result, err := sf.FormatMigration(m)
	assert.NoError(t, err)
	assert.Contains(t, result, "-- smf migration")
	assert.Contains(t, result, "-- No SQL statements generated")
}

func TestSQLFormatterFormatMigrationBasicStatement(t *testing.T) {
	sf := sqlFormatter{}
	m := &migration.Migration{}
	m.AddStatement("CREATE TABLE users (id INT)")
	result, err := sf.FormatMigration(m)
	assert.NoError(t, err)
	assert.Contains(t, result, "-- smf migration")
	assert.Contains(t, result, "-- SQL")
	assert.Contains(t, result, "CREATE TABLE users (id INT);")
}

func TestSQLFormatterFormatMigrationWithBreakingChanges(t *testing.T) {
	sf := sqlFormatter{}
	m := &migration.Migration{}
	m.AddBreaking("This is a breaking change")
	m.AddStatement("ALTER TABLE users DROP COLUMN email")
	result, err := sf.FormatMigration(m)
	assert.NoError(t, err)
	assert.Contains(t, result, "-- BREAKING CHANGES")
	assert.Contains(t, result, "-- - This is a breaking change")
	assert.Contains(t, result, "ALTER TABLE users DROP COLUMN email;")
}

func TestSQLFormatterFormatMigrationWithUnresolved(t *testing.T) {
	sf := sqlFormatter{}
	m := &migration.Migration{}
	m.AddUnresolved("Cannot determine rename")
	m.AddStatement("CREATE TABLE t (id INT)")
	result, err := sf.FormatMigration(m)
	assert.NoError(t, err)
	assert.Contains(t, result, "-- UNRESOLVED")
	assert.Contains(t, result, "-- - Cannot determine rename")
}

func TestSQLFormatterFormatMigrationWithNotes(t *testing.T) {
	sf := sqlFormatter{}
	m := &migration.Migration{}
	m.AddNote("Important note")
	m.AddStatement("CREATE TABLE t (id INT)")
	result, err := sf.FormatMigration(m)
	assert.NoError(t, err)
	assert.Contains(t, result, "-- NOTES")
	assert.Contains(t, result, "-- - Important note")
}

func TestSQLFormatterFormatMigrationWithRollback(t *testing.T) {
	sf := sqlFormatter{}
	m := &migration.Migration{}
	m.AddStatementWithRollback("CREATE TABLE users (id INT)", "DROP TABLE users")
	result, err := sf.FormatMigration(m)
	assert.NoError(t, err)
	assert.Contains(t, result, "CREATE TABLE users (id INT);")
	assert.Contains(t, result, "-- ROLLBACK SQL")
	assert.Contains(t, result, "-- DROP TABLE users;")
}

func TestSQLFormatterFormatMigrationMultipleStatements(t *testing.T) {
	sf := sqlFormatter{}
	m := &migration.Migration{}
	m.AddStatement("CREATE TABLE users (id INT)")
	m.AddStatement("CREATE TABLE posts (id INT)")
	m.AddStatement("ALTER TABLE posts ADD COLUMN user_id INT")
	result, err := sf.FormatMigration(m)
	assert.NoError(t, err)
	assert.Contains(t, result, "CREATE TABLE users (id INT);")
	assert.Contains(t, result, "CREATE TABLE posts (id INT);")
	assert.Contains(t, result, "ALTER TABLE posts ADD COLUMN user_id INT;")
}

func TestSQLFormatterFormatMigrationStatementWithSemicolon(t *testing.T) {
	sf := sqlFormatter{}
	m := &migration.Migration{}
	m.AddStatement("CREATE TABLE users (id INT);")
	result, err := sf.FormatMigration(m)
	assert.NoError(t, err)
	assert.Contains(t, result, "CREATE TABLE users (id INT);")
	assert.NotContains(t, result, "CREATE TABLE users (id INT);;")
}

func TestFormatRollbackSQLNil(t *testing.T) {
	result := FormatRollbackSQL(nil)
	assert.Equal(t, "", result)
}

func TestFormatRollbackSQLEmpty(t *testing.T) {
	m := &migration.Migration{}
	result := FormatRollbackSQL(m)
	assert.Contains(t, result, "-- smf rollback")
	assert.Contains(t, result, "-- No rollback statements generated")
}

func TestFormatRollbackSQLSingleStatement(t *testing.T) {
	m := &migration.Migration{}
	m.AddRollbackStatement("DROP TABLE users")
	result := FormatRollbackSQL(m)
	assert.Contains(t, result, "-- smf rollback")
	assert.Contains(t, result, "DROP TABLE users;")
}

func TestFormatRollbackSQLMultipleStatements(t *testing.T) {
	m := &migration.Migration{}
	m.AddRollbackStatement("DROP TABLE users")
	m.AddRollbackStatement("DROP TABLE posts")
	m.AddRollbackStatement("DROP TABLE comments")
	result := FormatRollbackSQL(m)
	assert.Contains(t, result, "DROP TABLE users;")
	assert.Contains(t, result, "DROP TABLE posts;")
	assert.Contains(t, result, "DROP TABLE comments;")
}

func TestFormatRollbackSQLReverseOrder(t *testing.T) {
	m := &migration.Migration{}
	m.AddStatementWithRollback("CREATE TABLE users (id INT)", "DROP TABLE users")
	m.AddStatementWithRollback("CREATE TABLE posts (id INT)", "DROP TABLE posts")
	result := FormatRollbackSQL(m)
	posUsers := strings.Index(result, "DROP TABLE users")
	posPosts := strings.Index(result, "DROP TABLE posts")
	assert.True(t, posPosts < posUsers)
}

func TestFormatRollbackSQLWithSemicolon(t *testing.T) {
	m := &migration.Migration{}
	m.AddRollbackStatement("DROP TABLE users;")
	result := FormatRollbackSQL(m)
	assert.Contains(t, result, "DROP TABLE users;")
	assert.NotContains(t, result, "DROP TABLE users;;")
}

func TestSQLFormatterWithEmptySQL(t *testing.T) {
	sf := sqlFormatter{}
	m := &migration.Migration{}
	m.Operations = append(m.Operations, core.Operation{
		Kind: core.OperationSQL,
		SQL:  "",
	})
	m.AddStatement("CREATE TABLE users (id INT)")

	result, err := sf.FormatMigration(m)
	assert.NoError(t, err)
	assert.Contains(t, result, "CREATE TABLE users (id INT);")
	assert.Equal(t, 1, strings.Count(result, "CREATE TABLE"))
}

func TestSQLFormatterWithRiskWarning(t *testing.T) {
	sf := sqlFormatter{}
	m := &migration.Migration{}
	m.Operations = append(m.Operations, core.Operation{
		Kind: core.OperationSQL,
		SQL:  "ALTER TABLE users MODIFY COLUMN email VARCHAR(100)",
		Risk: core.RiskWarning,
	})

	result, err := sf.FormatMigration(m)
	assert.NoError(t, err)
	assert.Contains(t, result, "-- [WARNING]")
	assert.Contains(t, result, "ALTER TABLE users MODIFY COLUMN email VARCHAR(100);")
}

func TestSQLFormatterWithRiskCritical(t *testing.T) {
	sf := sqlFormatter{}
	m := &migration.Migration{}
	m.Operations = append(m.Operations, core.Operation{
		Kind: core.OperationSQL,
		SQL:  "DROP TABLE users",
		Risk: core.RiskCritical,
	})

	result, err := sf.FormatMigration(m)
	assert.NoError(t, err)
	assert.Contains(t, result, "-- [CRITICAL]")
	assert.Contains(t, result, "DROP TABLE users;")
}

func TestSQLFormatterWithRequiresLock(t *testing.T) {
	sf := sqlFormatter{}
	m := &migration.Migration{}
	m.Operations = append(m.Operations, core.Operation{
		Kind:         core.OperationSQL,
		SQL:          "ALTER TABLE users ADD COLUMN status VARCHAR(50)",
		Risk:         core.RiskWarning,
		RequiresLock: true,
	})

	result, err := sf.FormatMigration(m)
	assert.NoError(t, err)
	assert.Contains(t, result, "-- [WARNING] (may acquire locks)")
	assert.Contains(t, result, "ALTER TABLE users ADD COLUMN status VARCHAR(50);")
}

func TestSQLFormatterWithRiskBreaking(t *testing.T) {
	sf := sqlFormatter{}
	m := &migration.Migration{}
	m.Operations = append(m.Operations, core.Operation{
		Kind: core.OperationSQL,
		SQL:  "ALTER TABLE users DROP COLUMN email",
		Risk: core.RiskBreaking,
	})

	result, err := sf.FormatMigration(m)
	assert.NoError(t, err)
	assert.Contains(t, result, "-- [BREAKING]")
	assert.Contains(t, result, "ALTER TABLE users DROP COLUMN email;")
}

func TestSQLFormatterWithRiskInfo(t *testing.T) {
	sf := sqlFormatter{}
	m := &migration.Migration{}
	m.Operations = append(m.Operations, core.Operation{
		Kind: core.OperationSQL,
		SQL:  "CREATE TABLE users (id INT)",
		Risk: core.RiskInfo,
	})

	result, err := sf.FormatMigration(m)
	assert.NoError(t, err)
	assert.NotContains(t, result, "-- [INFO]")
	assert.Contains(t, result, "CREATE TABLE users (id INT);")
}

func TestFormatRollbackSQLWithEmptyStatements(t *testing.T) {
	m := &migration.Migration{}
	m.AddRollbackStatement("DROP TABLE users")
	m.AddRollbackStatement("")
	m.AddRollbackStatement("   ")
	m.AddRollbackStatement("DROP TABLE posts")

	result := FormatRollbackSQL(m)
	assert.Contains(t, result, "DROP TABLE users;")
	assert.Contains(t, result, "DROP TABLE posts;")
	assert.Equal(t, 2, strings.Count(result, "DROP TABLE"))
}

func TestSplitCommentLinesWithEmptyLines(t *testing.T) {
	m := &migration.Migration{}
	m.AddNote("line1\n\nline2\n   \nline3")
	m.AddStatement("CREATE TABLE users (id INT)")

	sf := sqlFormatter{}
	result, err := sf.FormatMigration(m)
	assert.NoError(t, err)
	assert.Contains(t, result, "-- - line1")
	assert.Contains(t, result, "-- - line2")
	assert.Contains(t, result, "-- - line3")
	assert.NotContains(t, result, "-- - \n")
}

func TestWriteRollbackAsCommentsWithEmptyLines(t *testing.T) {
	m := &migration.Migration{}
	m.AddRollbackStatement("DROP TABLE users\n\nDROP INDEX idx_email")
	m.AddStatement("CREATE TABLE users (id INT)")

	sf := sqlFormatter{}
	result, err := sf.FormatMigration(m)
	assert.NoError(t, err)
	assert.Contains(t, result, "-- ROLLBACK SQL")
	assert.Contains(t, result, "-- DROP TABLE users;")
	assert.Contains(t, result, "-- DROP INDEX idx_email;")
}
