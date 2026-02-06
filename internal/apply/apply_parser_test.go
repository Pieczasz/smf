package apply

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSplitStatementsBySemicolon(t *testing.T) {
	t.Run("basic split", func(t *testing.T) {
		stmts := splitStatementsBySemicolon("SELECT 1;\nSELECT 2;")
		assert.Len(t, stmts, 2)
	})

	t.Run("skip comments", func(t *testing.T) {
		stmts := splitStatementsBySemicolon("-- comment\nSELECT 1;\n-- another\nSELECT 2;")
		assert.Len(t, stmts, 2)
	})

	t.Run("handles trailing content without semicolon", func(t *testing.T) {
		stmts := splitStatementsBySemicolon("SELECT 1;\nSELECT 2")
		assert.Len(t, stmts, 2)
	})

	t.Run("empty input", func(t *testing.T) {
		stmts := splitStatementsBySemicolon("")
		assert.Empty(t, stmts)
	})

	t.Run("only comments and blanks", func(t *testing.T) {
		stmts := splitStatementsBySemicolon("-- just a comment\n\n-- another one")
		assert.Empty(t, stmts)
	})
}

func TestTruncateSQL(t *testing.T) {
	t.Run("short statement unchanged", func(t *testing.T) {
		assert.Equal(t, "SELECT 1", truncateSQL("SELECT 1", 60))
	})

	t.Run("long statement truncated", func(t *testing.T) {
		long := strings.Repeat("A", 100)
		result := truncateSQL(long, 20)
		assert.Len(t, result, 20)
		assert.True(t, strings.HasSuffix(result, "..."))
	})

	t.Run("default max when zero", func(t *testing.T) {
		long := strings.Repeat("A", 100)
		result := truncateSQL(long, 0)
		assert.Len(t, result, 60)
	})

	t.Run("negative max treated as default", func(t *testing.T) {
		long := strings.Repeat("A", 100)
		result := truncateSQL(long, -5)
		assert.Len(t, result, 60)
	})

	t.Run("trims whitespace", func(t *testing.T) {
		result := truncateSQL("  SELECT 1  ", 60)
		assert.Equal(t, "SELECT 1", result)
	})
}

func TestApplierParserFallback(t *testing.T) {
	applier := NewApplier(Options{})

	t.Run("valid SQL parsed by TiDB", func(t *testing.T) {
		stmts := applier.splitStatementsWithParser("SELECT 1; SELECT 2;")
		assert.Len(t, stmts, 2)
	})

	t.Run("unparseable SQL falls back to semicolon split", func(t *testing.T) {
		stmts := applier.splitStatementsWithParser("SOME_CUSTOM_NONSENSE arg1 arg2;\nANOTHER_NONSENSE arg3;")
		assert.Len(t, stmts, 2)
	})

	t.Run("empty input", func(t *testing.T) {
		stmts := applier.splitStatementsWithParser("")
		assert.Empty(t, stmts)
	})
}

func TestSplitStatementsUsingTiDBParserEdgeCases(t *testing.T) {
	applier := NewApplier(Options{})

	t.Run("empty string returns nil", func(t *testing.T) {
		stmts := applier.splitStatementsUsingTiDBParser("")
		assert.Nil(t, stmts)
	})

	t.Run("comment-only returns nil", func(t *testing.T) {
		stmts := applier.splitStatementsUsingTiDBParser("-- just a comment")
		assert.Nil(t, stmts)
	})

	t.Run("valid multi-statement", func(t *testing.T) {
		stmts := applier.splitStatementsUsingTiDBParser("SELECT 1; SELECT 2;")
		assert.Len(t, stmts, 2)
	})
}
func TestApplierParseStatements(t *testing.T) {
	applier := NewApplier(Options{})

	t.Run("parse SQL text", func(t *testing.T) {
		parseSQLTextTest(t, applier)
	})

	t.Run("parse JSON migration format", func(t *testing.T) {
		parseJSONMigrationTest(t, applier)
	})

	t.Run("parse JSON with empty SQL falls back to SQL parsing", func(t *testing.T) {
		parseJSONEmptyTest(t, applier)
	})

	t.Run("parse SQL with comments and blank lines", func(t *testing.T) {
		parseSQLCommentsTest(t, applier)
	})

	t.Run("parse single statement without semicolon", func(t *testing.T) {
		parseSingleStatementTest(t, applier)
	})

	t.Run("empty content produces no statements", func(t *testing.T) {
		parseEmptyTest(t, applier)
	})

	t.Run("whitespace-only content produces no statements", func(t *testing.T) {
		parseWhitespaceTest(t, applier)
	})

	t.Run("parse JSON with whitespace-only SQL entries", func(t *testing.T) {
		parseJSONWhitespaceTest(t, applier)
	})

	t.Run("invalid JSON falls back to SQL parsing", func(t *testing.T) {
		parseInvalidJSONTest(t, applier)
	})
}

func parseSQLTextTest(t *testing.T, applier *Applier) {
	t.Helper()
	sql := `
		CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255));
		ALTER TABLE users ADD COLUMN email VARCHAR(255);
	`
	stmts := applier.ParseStatements(sql)
	require.Len(t, stmts, 2)
	assert.Contains(t, strings.ToUpper(stmts[0]), "CREATE TABLE")
	assert.Contains(t, strings.ToUpper(stmts[1]), "ALTER TABLE")
}

func parseJSONMigrationTest(t *testing.T, applier *Applier) {
	t.Helper()
	migration := jsonMigration{
		Format: "json",
		SQL: []string{
			"CREATE TABLE orders (id INT PRIMARY KEY)",
			"ALTER TABLE orders ADD COLUMN total DECIMAL(10,2)",
		},
	}
	migration.Summary.SQLStatements = 2
	raw, err := json.Marshal(migration)
	require.NoError(t, err)

	stmts := applier.ParseStatements(string(raw))
	require.Len(t, stmts, 2)
	assert.Contains(t, stmts[0], "CREATE TABLE")
	assert.Contains(t, stmts[1], "ALTER TABLE")
}

func parseJSONEmptyTest(t *testing.T, applier *Applier) {
	t.Helper()
	raw := `{"format":"json","sql":[],"summary":{"sqlStatements":0}}`
	stmts := applier.ParseStatements(raw)
	assert.Empty(t, stmts)
}

func parseSQLCommentsTest(t *testing.T, applier *Applier) {
	t.Helper()
	sql := `
-- This is a comment
CREATE TABLE test1 (id INT PRIMARY KEY);

-- Another comment

CREATE TABLE test2 (id INT PRIMARY KEY);
`
	stmts := applier.ParseStatements(sql)
	require.Len(t, stmts, 2)
}

func parseSingleStatementTest(t *testing.T, applier *Applier) {
	t.Helper()
	sql := "CREATE TABLE test1 (id INT PRIMARY KEY)"
	stmts := applier.ParseStatements(sql)
	require.Len(t, stmts, 1)
}

func parseEmptyTest(t *testing.T, applier *Applier) {
	t.Helper()
	stmts := applier.ParseStatements("")
	assert.Empty(t, stmts)
}

func parseWhitespaceTest(t *testing.T, applier *Applier) {
	t.Helper()
	stmts := applier.ParseStatements("   \n\n  \t  ")
	assert.Empty(t, stmts)
}

func parseJSONWhitespaceTest(t *testing.T, applier *Applier) {
	t.Helper()
	raw := `{"format":"json","sql":["  ", "", "CREATE TABLE t (id INT)"],"summary":{"sqlStatements":1}}`
	stmts := applier.ParseStatements(raw)
	require.Len(t, stmts, 1)
	assert.Contains(t, stmts[0], "CREATE TABLE")
}

func parseInvalidJSONTest(t *testing.T, applier *Applier) {
	t.Helper()
	sql := "CREATE TABLE test1 (id INT PRIMARY KEY);"
	stmts := applier.ParseStatements(sql)
	require.Len(t, stmts, 1)
}
