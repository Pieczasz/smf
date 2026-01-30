package diff

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"smf/internal/parser"
)

func TestDiff(t *testing.T) {
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

	d := Diff(oldDB, newDB, DefaultOptions())
	require.NotNil(t, d)

	assert.Len(t, d.AddedTables, 1, "Expected 1 added table")
	assert.Equal(t, "comments", d.AddedTables[0].Name)

	assert.Len(t, d.RemovedTables, 1, "Expected 1 removed table")
	assert.Equal(t, "posts", d.RemovedTables[0].Name)

	assert.Len(t, d.ModifiedTables, 1, "Expected 1 modified table")
	assert.Equal(t, "users", d.ModifiedTables[0].Name)
}
