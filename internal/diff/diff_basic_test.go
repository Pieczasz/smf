package diff

import (
	"os"
	"smf/internal/parser"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

	d := Diff(oldDB, newDB)
	require.NotNil(t, d)

	s := d.String()

	assert.Contains(t, s, "Added tables")
	assert.Contains(t, s, "comments")
	assert.Contains(t, s, "Removed tables")
	assert.Contains(t, s, "posts")
	assert.Contains(t, s, "Modified tables")
	assert.Contains(t, s, "users")
	assert.Contains(t, s, "Added columns")
	assert.Contains(t, s, "email")
	assert.Contains(t, s, "Modified columns")
	assert.Contains(t, s, "name")

	f, err := os.CreateTemp("", "smf-diff-*.txt")
	require.NoError(t, err)
	defer func(name string) {
		err := os.Remove(name)
		if err != nil {
		}
	}(f.Name())

	err = f.Close()
	if err != nil {
		return
	}

	require.NoError(t, d.SaveToFile(f.Name()))
	b, err := os.ReadFile(f.Name())
	require.NoError(t, err)
	assert.Contains(t, string(b), "Schema differences")
}
