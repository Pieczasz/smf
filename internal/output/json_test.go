package output

import (
	"flag"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"smf/internal/dialect"
	"smf/internal/dialect/mysql"
	"smf/internal/diff"
	"smf/internal/parser"
)

var updateGolden = flag.Bool("update-golden", false, "update golden files")

const (
	jsonOldSchema = `CREATE TABLE users (
		id INT PRIMARY KEY AUTO_INCREMENT,
		name VARCHAR(255) NULL
	);

	CREATE TABLE posts (
		id INT PRIMARY KEY
	);`

	jsonNewSchema = `CREATE TABLE users (
		id INT PRIMARY KEY AUTO_INCREMENT,
		name VARCHAR(255) NOT NULL,
		email VARCHAR(255)
	);

	CREATE TABLE comments (
		id INT PRIMARY KEY
	);`
)

func TestDiffJSONFormatGolden(t *testing.T) {
	out := formatDiffJSON(t)
	if *updateGolden {
		writeGolden(t, "diff_golden.json", out)
		return
	}
	require.Equal(t, readGolden(t, "diff_golden.json"), out)
}

func TestMigrationJSONFormatGolden(t *testing.T) {
	out := formatMigrationJSON(t)
	if *updateGolden {
		writeGolden(t, "migration_golden.json", out)
		return
	}
	require.Equal(t, readGolden(t, "migration_golden.json"), out)
}

func formatDiffJSON(t *testing.T) string {
	t.Helper()
	p := parser.NewSQLParser()
	oldDB, err := p.ParseSchema(jsonOldSchema)
	require.NoError(t, err)
	newDB, err := p.ParseSchema(jsonNewSchema)
	require.NoError(t, err)

	d := diff.Diff(oldDB, newDB, diff.DefaultOptions())
	formatter, err := NewFormatter("json")
	require.NoError(t, err)
	out, err := formatter.FormatDiff(d)
	require.NoError(t, err)
	return out
}

func formatMigrationJSON(t *testing.T) string {
	t.Helper()
	p := parser.NewSQLParser()
	oldDB, err := p.ParseSchema(jsonOldSchema)
	require.NoError(t, err)
	newDB, err := p.ParseSchema(jsonNewSchema)
	require.NoError(t, err)

	schemaDiff := diff.Diff(oldDB, newDB, diff.DefaultOptions())
	d := mysql.NewMySQLDialect()
	opts := dialect.DefaultMigrationOptions(dialect.MySQL)
	migration := d.Generator().GenerateMigration(schemaDiff, opts)

	formatter, err := NewFormatter("json")
	require.NoError(t, err)
	out, err := formatter.FormatMigration(migration)
	require.NoError(t, err)
	return out
}

func readGolden(t *testing.T, name string) string {
	t.Helper()
	path := filepath.Join("..", "..", "test", "data", name)
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	return string(b)
}

func writeGolden(t *testing.T, name string, content string) {
	t.Helper()
	path := filepath.Join("..", "..", "test", "data", name)
	err := os.WriteFile(path, []byte(content), 0644)
	require.NoError(t, err)
	t.Logf("Updated golden file: %s", path)
}
