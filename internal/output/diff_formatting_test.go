package output

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
