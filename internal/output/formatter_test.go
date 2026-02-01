package output

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewFormatterDefaultsToSQL(t *testing.T) {
	f, err := NewFormatter("")
	require.NoError(t, err)
	_, ok := f.(sqlFormatter)
	assert.True(t, ok)
}

func TestNewFormatterSQL(t *testing.T) {
	f, err := NewFormatter("sql")
	require.NoError(t, err)
	_, ok := f.(sqlFormatter)
	assert.True(t, ok)
}

func TestNewFormatterSQLUppercase(t *testing.T) {
	f, err := NewFormatter("SQL")
	require.NoError(t, err)
	_, ok := f.(sqlFormatter)
	assert.True(t, ok)
}

func TestNewFormatterJSON(t *testing.T) {
	f, err := NewFormatter("json")
	require.NoError(t, err)
	_, ok := f.(jsonFormatter)
	assert.True(t, ok)
}

func TestNewFormatterJSONUppercase(t *testing.T) {
	f, err := NewFormatter("JSON")
	require.NoError(t, err)
	_, ok := f.(jsonFormatter)
	assert.True(t, ok)
}

func TestNewFormatterSummary(t *testing.T) {
	f, err := NewFormatter("summary")
	require.NoError(t, err)
	_, ok := f.(summaryFormatter)
	assert.True(t, ok)
}

func TestNewFormatterWithWhitespace(t *testing.T) {
	f, err := NewFormatter("  sql  ")
	require.NoError(t, err)
	_, ok := f.(sqlFormatter)
	assert.True(t, ok)
}

func TestNewFormatterInvalidFormat(t *testing.T) {
	f, err := NewFormatter("invalid")
	assert.Error(t, err)
	assert.Nil(t, f)
	assert.Contains(t, err.Error(), "unsupported format: invalid")
}

func TestNewFormatterInvalidFormatWithMessage(t *testing.T) {
	f, err := NewFormatter("yaml")
	assert.Error(t, err)
	assert.Nil(t, f)
	assert.Contains(t, err.Error(), "use 'sql', 'json', or 'summary'")
}

func TestNormalizeStatementsEmpty(t *testing.T) {
	result := normalizeStatements([]string{})
	assert.Empty(t, result)
}

func TestNormalizeStatementsRemovesEmptyStrings(t *testing.T) {
	input := []string{"CREATE TABLE t", "", "   ", "DROP TABLE t"}
	result := normalizeStatements(input)
	assert.Len(t, result, 2)
	assert.Equal(t, "CREATE TABLE t;", result[0])
	assert.Equal(t, "DROP TABLE t;", result[1])
}

func TestNormalizeStatementsAddsSemicolon(t *testing.T) {
	input := []string{"CREATE TABLE t"}
	result := normalizeStatements(input)
	assert.Len(t, result, 1)
	assert.Equal(t, "CREATE TABLE t;", result[0])
}

func TestNormalizeStatementsKeepsSemicolon(t *testing.T) {
	input := []string{"CREATE TABLE t;"}
	result := normalizeStatements(input)
	assert.Len(t, result, 1)
	assert.Equal(t, "CREATE TABLE t;", result[0])
}

func TestNormalizeStatementsTrimsSingleStatement(t *testing.T) {
	input := []string{"  CREATE TABLE t  "}
	result := normalizeStatements(input)
	assert.Len(t, result, 1)
	assert.Equal(t, "CREATE TABLE t;", result[0])
}

func TestNormalizeStatementsMultiple(t *testing.T) {
	input := []string{"CREATE TABLE users", "CREATE TABLE posts;", "  ALTER TABLE users  ", "", "DROP TABLE old"}
	result := normalizeStatements(input)
	assert.Len(t, result, 4)
	assert.Equal(t, "CREATE TABLE users;", result[0])
	assert.Equal(t, "CREATE TABLE posts;", result[1])
	assert.Equal(t, "ALTER TABLE users;", result[2])
	assert.Equal(t, "DROP TABLE old;", result[3])
}

func TestReverseStatementsEmpty(t *testing.T) {
	result := reverseStatements([]string{})
	assert.Nil(t, result)
}

func TestReverseStatementsNil(t *testing.T) {
	result := reverseStatements(nil)
	assert.Nil(t, result)
}

func TestReverseStatementsSingle(t *testing.T) {
	input := []string{"CREATE TABLE t"}
	result := reverseStatements(input)
	assert.Len(t, result, 1)
	assert.Equal(t, "CREATE TABLE t", result[0])
}

func TestReverseStatementsMultiple(t *testing.T) {
	input := []string{"first", "second", "third"}
	result := reverseStatements(input)
	assert.Len(t, result, 3)
	assert.Equal(t, "third", result[0])
	assert.Equal(t, "second", result[1])
	assert.Equal(t, "first", result[2])
}

func TestReverseStatementsDoesNotModifyOriginal(t *testing.T) {
	input := []string{"first", "second", "third"}
	result := reverseStatements(input)
	assert.Equal(t, "first", input[0])
	assert.Equal(t, "third", result[0])
}
