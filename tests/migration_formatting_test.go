package tests

import (
	"os"
	"smf/migration"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMigrationStringMultiLineNotesAreCommented(t *testing.T) {
	m := &migration.Migration{}
	m.AddNote("line1\nline2")
	m.AddRollbackStatement("ALTER TABLE t ADD COLUMN c INT")

	out := m.String()
	assert.Contains(t, out, "-- NOTES")
	assert.Contains(t, out, "-- - line1")
	assert.Contains(t, out, "-- - line2")
	assert.NotContains(t, out, "\nline2\n")
	assert.Contains(t, out, "-- ROLLBACK SQL")
	assert.Contains(t, out, "-- ALTER TABLE t ADD COLUMN c INT;")

	rb := m.RollbackString()
	assert.Contains(t, rb, "-- smf rollback")
	assert.Contains(t, rb, "ALTER TABLE t ADD COLUMN c INT;")
}

func TestMigrationSaveRollbackToFileWritesRollbackSQL(t *testing.T) {
	m := &migration.Migration{}
	m.AddRollbackStatement("ALTER TABLE t ADD COLUMN c INT")

	f, err := os.CreateTemp("", "smf-rollback-*.sql")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	name := f.Name()
	_ = f.Close()
	defer func() { _ = os.Remove(name) }()

	assert.NoError(t, m.SaveRollbackToFile(name))
	b, err := os.ReadFile(name)
	assert.NoError(t, err)
	out := string(b)
	assert.Contains(t, out, "-- smf rollback")
	assert.Contains(t, out, "ALTER TABLE t ADD COLUMN c INT;")
}
