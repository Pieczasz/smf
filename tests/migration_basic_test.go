package tests

import (
	"os"
	"smf/dialect/mysql"
	"smf/diff"
	"testing"

	"smf/parser"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBasicMigration(t *testing.T) {
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

	d := diff.Diff(oldDB, newDB)
	require.NotNil(t, d)

	mysqlDialect := mysql.NewMySQLDialect()
	mig := mysqlDialect.Generator().GenerateMigration(d)
	require.NotNil(t, mig)

	out := mig.String()
	assert.Contains(t, out, "-- SQL")
	assert.Contains(t, out, "CREATE TABLE")
	assert.Contains(t, out, "ALTER TABLE")
	assert.Contains(t, out, "DROP TABLE")
	assert.Contains(t, out, "BREAKING CHANGES")

	f, err := os.CreateTemp("", "smf-migration-*.sql")
	require.NoError(t, err)
	name := f.Name()
	require.NoError(t, f.Close())
	defer func() { _ = os.Remove(name) }()

	require.NoError(t, mig.SaveToFile(name))
	b, err := os.ReadFile(name)
	require.NoError(t, err)
	assert.Contains(t, string(b), "-- smf migration")
}
