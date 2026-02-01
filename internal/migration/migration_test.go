package migration

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"smf/internal/core"
)

// opsToStringsTest is a reusable test case for testing methods that convert operations to string slices.
type opsToStringsTest struct {
	name       string
	operations []core.Operation
	want       []string
}

func TestMigrationPlan(t *testing.T) {
	tests := []struct {
		name       string
		operations []core.Operation
		want       []core.Operation
	}{
		{
			name:       "empty operations",
			operations: nil,
			want:       nil,
		},
		{
			name: "single operation",
			operations: []core.Operation{
				{Kind: core.OperationSQL, SQL: "CREATE TABLE users (id INT)"},
			},
			want: []core.Operation{
				{Kind: core.OperationSQL, SQL: "CREATE TABLE users (id INT)"},
			},
		},
		{
			name: "multiple operations",
			operations: []core.Operation{
				{Kind: core.OperationSQL, SQL: "CREATE TABLE users (id INT)"},
				{Kind: core.OperationNote, SQL: "Added users table"},
				{Kind: core.OperationBreaking, SQL: "Breaking change"},
			},
			want: []core.Operation{
				{Kind: core.OperationSQL, SQL: "CREATE TABLE users (id INT)"},
				{Kind: core.OperationNote, SQL: "Added users table"},
				{Kind: core.OperationBreaking, SQL: "Breaking change"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Migration{Operations: tt.operations}
			assert.Equal(t, tt.want, m.Plan())
		})
	}
}

func TestMigrationSQLStatements(t *testing.T) {
	tests := []opsToStringsTest{
		{"empty operations", nil, []string{}},
		{
			name: "single SQL operation",
			operations: []core.Operation{
				{Kind: core.OperationSQL, SQL: "CREATE TABLE users (id INT)"},
			},
			want: []string{"CREATE TABLE users (id INT)"},
		},
		{
			name: "multiple SQL operations",
			operations: []core.Operation{
				{Kind: core.OperationSQL, SQL: "CREATE TABLE users (id INT)"},
				{Kind: core.OperationSQL, SQL: "ALTER TABLE users ADD name VARCHAR(255)"},
			},
			want: []string{"CREATE TABLE users (id INT)", "ALTER TABLE users ADD name VARCHAR(255)"},
		},
		{
			name: "mixed operations - only SQL returned",
			operations: []core.Operation{
				{Kind: core.OperationSQL, SQL: "CREATE TABLE users (id INT)"},
				{Kind: core.OperationNote, SQL: "This is a note"},
				{Kind: core.OperationBreaking, SQL: "Breaking change"},
				{Kind: core.OperationSQL, SQL: "DROP TABLE old_table"},
			},
			want: []string{"CREATE TABLE users (id INT)", "DROP TABLE old_table"},
		},
		{
			name: "SQL with whitespace trimmed",
			operations: []core.Operation{
				{Kind: core.OperationSQL, SQL: "  CREATE TABLE users (id INT)  "},
			},
			want: []string{"CREATE TABLE users (id INT)"},
		},
		{
			name: "empty SQL is skipped",
			operations: []core.Operation{
				{Kind: core.OperationSQL, SQL: "CREATE TABLE users (id INT)"},
				{Kind: core.OperationSQL, SQL: "   "},
				{Kind: core.OperationSQL, SQL: ""},
			},
			want: []string{"CREATE TABLE users (id INT)"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Migration{Operations: tt.operations}
			assert.Equal(t, tt.want, m.SQLStatements())
		})
	}
}

func TestMigrationRollbackStatements(t *testing.T) {
	tests := []opsToStringsTest{
		{"empty operations", nil, []string{}},
		{
			name: "single rollback statement",
			operations: []core.Operation{
				{Kind: core.OperationSQL, SQL: "CREATE TABLE users (id INT)", RollbackSQL: "DROP TABLE users"},
			},
			want: []string{"DROP TABLE users"},
		},
		{
			name: "multiple rollback statements",
			operations: []core.Operation{
				{Kind: core.OperationSQL, SQL: "CREATE TABLE users (id INT)", RollbackSQL: "DROP TABLE users"},
				{Kind: core.OperationSQL, SQL: "CREATE TABLE posts (id INT)", RollbackSQL: "DROP TABLE posts"},
			},
			want: []string{"DROP TABLE users", "DROP TABLE posts"},
		},
		{
			name: "operations without rollback are skipped",
			operations: []core.Operation{
				{Kind: core.OperationSQL, SQL: "CREATE TABLE users (id INT)", RollbackSQL: "DROP TABLE users"},
				{Kind: core.OperationSQL, SQL: "INSERT INTO users VALUES (1)"},
				{Kind: core.OperationSQL, SQL: "CREATE TABLE posts (id INT)", RollbackSQL: "DROP TABLE posts"},
			},
			want: []string{"DROP TABLE users", "DROP TABLE posts"},
		},
		{
			name: "non-SQL operations ignored",
			operations: []core.Operation{
				{Kind: core.OperationNote, SQL: "note", RollbackSQL: "should not appear"},
				{Kind: core.OperationSQL, RollbackSQL: "DROP TABLE users"},
			},
			want: []string{"DROP TABLE users"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Migration{Operations: tt.operations}
			assert.Equal(t, tt.want, m.RollbackStatements())
		})
	}
}

func TestMigrationBreakingNotes(t *testing.T) {
	tests := []opsToStringsTest{
		{"empty operations", nil, []string{}},
		{
			name: "single breaking note",
			operations: []core.Operation{
				{Kind: core.OperationBreaking, SQL: "Column dropped", Risk: core.RiskBreaking},
			},
			want: []string{"Column dropped"},
		},
		{
			name: "multiple breaking notes",
			operations: []core.Operation{
				{Kind: core.OperationBreaking, SQL: "Column dropped", Risk: core.RiskBreaking},
				{Kind: core.OperationBreaking, SQL: "Table renamed", Risk: core.RiskBreaking},
			},
			want: []string{"Column dropped", "Table renamed"},
		},
		{
			name: "mixed operations - only breaking returned",
			operations: []core.Operation{
				{Kind: core.OperationSQL, SQL: "DROP COLUMN name"},
				{Kind: core.OperationBreaking, SQL: "Column dropped", Risk: core.RiskBreaking},
				{Kind: core.OperationNote, SQL: "Some note"},
			},
			want: []string{"Column dropped"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Migration{Operations: tt.operations}
			assert.Equal(t, tt.want, m.BreakingNotes())
		})
	}
}

func TestMigrationUnresolvedNotes(t *testing.T) {
	tests := []opsToStringsTest{
		{"empty operations", nil, []string{}},
		{
			name: "single unresolved note",
			operations: []core.Operation{
				{Kind: core.OperationUnresolved, UnresolvedReason: "Cannot determine column type"},
			},
			want: []string{"Cannot determine column type"},
		},
		{
			name: "multiple unresolved notes",
			operations: []core.Operation{
				{Kind: core.OperationUnresolved, UnresolvedReason: "Cannot determine column type"},
				{Kind: core.OperationUnresolved, UnresolvedReason: "Foreign key conflict"},
			},
			want: []string{"Cannot determine column type", "Foreign key conflict"},
		},
		{
			name: "mixed operations - only unresolved returned",
			operations: []core.Operation{
				{Kind: core.OperationSQL, SQL: "ALTER TABLE users"},
				{Kind: core.OperationUnresolved, UnresolvedReason: "Cannot resolve"},
				{Kind: core.OperationNote, SQL: "Info note"},
			},
			want: []string{"Cannot resolve"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Migration{Operations: tt.operations}
			assert.Equal(t, tt.want, m.UnresolvedNotes())
		})
	}
}

func TestMigrationInfoNotes(t *testing.T) {
	tests := []opsToStringsTest{
		{"empty operations", nil, []string{}},
		{
			name: "single info note",
			operations: []core.Operation{
				{Kind: core.OperationNote, SQL: "Migration adds new index", Risk: core.RiskInfo},
			},
			want: []string{"Migration adds new index"},
		},
		{
			name: "multiple info notes",
			operations: []core.Operation{
				{Kind: core.OperationNote, SQL: "Migration adds new index", Risk: core.RiskInfo},
				{Kind: core.OperationNote, SQL: "Consider adding constraint", Risk: core.RiskInfo},
			},
			want: []string{"Migration adds new index", "Consider adding constraint"},
		},
		{
			name: "mixed operations - only notes returned",
			operations: []core.Operation{
				{Kind: core.OperationSQL, SQL: "CREATE INDEX idx ON users(name)"},
				{Kind: core.OperationNote, SQL: "Index creation may take time", Risk: core.RiskInfo},
				{Kind: core.OperationBreaking, SQL: "Breaking"},
			},
			want: []string{"Index creation may take time"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Migration{Operations: tt.operations}
			assert.Equal(t, tt.want, m.InfoNotes())
		})
	}
}

func TestMigrationAddStatement(t *testing.T) {
	tests := []struct {
		name string
		stmt string
		want []core.Operation
	}{
		{name: "empty statement is ignored", stmt: "", want: nil},
		{name: "whitespace only statement is ignored", stmt: "   ", want: nil},
		{
			name: "valid statement is added",
			stmt: "CREATE TABLE users (id INT)",
			want: []core.Operation{{Kind: core.OperationSQL, SQL: "CREATE TABLE users (id INT)"}},
		},
		{
			name: "statement with whitespace is trimmed",
			stmt: "  CREATE TABLE users (id INT)  ",
			want: []core.Operation{{Kind: core.OperationSQL, SQL: "CREATE TABLE users (id INT)"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Migration{}
			m.AddStatement(tt.stmt)
			assert.Equal(t, tt.want, m.Operations)
		})
	}
}

func TestMigrationAddRollbackStatement(t *testing.T) {
	tests := []struct {
		name string
		stmt string
		want []core.Operation
	}{
		{name: "empty statement is ignored", stmt: "", want: nil},
		{name: "whitespace only statement is ignored", stmt: "   ", want: nil},
		{
			name: "valid rollback statement is added",
			stmt: "DROP TABLE users",
			want: []core.Operation{{Kind: core.OperationSQL, RollbackSQL: "DROP TABLE users"}},
		},
		{
			name: "rollback statement with whitespace is trimmed",
			stmt: "  DROP TABLE users  ",
			want: []core.Operation{{Kind: core.OperationSQL, RollbackSQL: "DROP TABLE users"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Migration{}
			m.AddRollbackStatement(tt.stmt)
			assert.Equal(t, tt.want, m.Operations)
		})
	}
}

func TestMigrationAddStatementWithRollback(t *testing.T) {
	tests := []struct {
		name string
		up   string
		down string
		want []core.Operation
	}{
		{name: "both empty are ignored", up: "", down: "", want: nil},
		{name: "both whitespace only are ignored", up: "   ", down: "   ", want: nil},
		{
			name: "valid up and down statements",
			up:   "CREATE TABLE users (id INT)",
			down: "DROP TABLE users",
			want: []core.Operation{{Kind: core.OperationSQL, SQL: "CREATE TABLE users (id INT)", RollbackSQL: "DROP TABLE users"}},
		},
		{
			name: "only up statement",
			up:   "CREATE TABLE users (id INT)",
			down: "",
			want: []core.Operation{{Kind: core.OperationSQL, SQL: "CREATE TABLE users (id INT)", RollbackSQL: ""}},
		},
		{
			name: "only down statement",
			up:   "",
			down: "DROP TABLE users",
			want: []core.Operation{{Kind: core.OperationSQL, SQL: "", RollbackSQL: "DROP TABLE users"}},
		},
		{
			name: "statements with whitespace are trimmed",
			up:   "  CREATE TABLE users (id INT)  ",
			down: "  DROP TABLE users  ",
			want: []core.Operation{{Kind: core.OperationSQL, SQL: "CREATE TABLE users (id INT)", RollbackSQL: "DROP TABLE users"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Migration{}
			m.AddStatementWithRollback(tt.up, tt.down)
			assert.Equal(t, tt.want, m.Operations)
		})
	}
}

func TestMigrationAddBreaking(t *testing.T) {
	tests := []struct {
		name string
		msg  string
		want []core.Operation
	}{
		{name: "empty message is ignored", msg: "", want: nil},
		{name: "whitespace only message is ignored", msg: "   ", want: nil},
		{
			name: "valid breaking message is added",
			msg:  "Column 'name' was dropped",
			want: []core.Operation{{Kind: core.OperationBreaking, SQL: "Column 'name' was dropped", Risk: core.RiskBreaking}},
		},
		{
			name: "message with whitespace is trimmed",
			msg:  "  Column dropped  ",
			want: []core.Operation{{Kind: core.OperationBreaking, SQL: "Column dropped", Risk: core.RiskBreaking}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Migration{}
			m.AddBreaking(tt.msg)
			assert.Equal(t, tt.want, m.Operations)
		})
	}
}

func TestMigrationAddNote(t *testing.T) {
	tests := []struct {
		name string
		msg  string
		want []core.Operation
	}{
		{name: "empty message is ignored", msg: "", want: nil},
		{name: "whitespace only message is ignored", msg: "   ", want: nil},
		{
			name: "valid note message is added",
			msg:  "Consider adding an index",
			want: []core.Operation{{Kind: core.OperationNote, SQL: "Consider adding an index", Risk: core.RiskInfo}},
		},
		{
			name: "message with whitespace is trimmed",
			msg:  "  Note message  ",
			want: []core.Operation{{Kind: core.OperationNote, SQL: "Note message", Risk: core.RiskInfo}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Migration{}
			m.AddNote(tt.msg)
			assert.Equal(t, tt.want, m.Operations)
		})
	}
}

func TestMigrationAddUnresolved(t *testing.T) {
	tests := []struct {
		name string
		msg  string
		want []core.Operation
	}{
		{name: "empty message is ignored", msg: "", want: nil},
		{name: "whitespace only message is ignored", msg: "   ", want: nil},
		{
			name: "valid unresolved message is added",
			msg:  "Cannot determine column type",
			want: []core.Operation{{Kind: core.OperationUnresolved, UnresolvedReason: "Cannot determine column type"}},
		},
		{
			name: "message with whitespace is trimmed",
			msg:  "  Unresolved issue  ",
			want: []core.Operation{{Kind: core.OperationUnresolved, UnresolvedReason: "Unresolved issue"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Migration{}
			m.AddUnresolved(tt.msg)
			assert.Equal(t, tt.want, m.Operations)
		})
	}
}

var migrationDedupeTests = []struct {
	name       string
	operations []core.Operation
	want       []core.Operation
}{
	{name: "empty operations", operations: nil, want: nil},
	{
		name: "no duplicates - unchanged",
		operations: []core.Operation{
			{Kind: core.OperationSQL, SQL: "CREATE TABLE users"},
			{Kind: core.OperationNote, SQL: "Note 1"},
			{Kind: core.OperationBreaking, SQL: "Breaking 1"},
		},
		want: []core.Operation{
			{Kind: core.OperationSQL, SQL: "CREATE TABLE users"},
			{Kind: core.OperationNote, SQL: "Note 1"},
			{Kind: core.OperationBreaking, SQL: "Breaking 1"},
		},
	},
	{
		name: "duplicate notes are removed",
		operations: []core.Operation{
			{Kind: core.OperationNote, SQL: "Same note"},
			{Kind: core.OperationNote, SQL: "Same note"},
			{Kind: core.OperationNote, SQL: "Different note"},
		},
		want: []core.Operation{
			{Kind: core.OperationNote, SQL: "Same note"},
			{Kind: core.OperationNote, SQL: "Different note"},
		},
	},
	{
		name: "duplicate breaking notes are removed",
		operations: []core.Operation{
			{Kind: core.OperationBreaking, SQL: "Breaking change"},
			{Kind: core.OperationBreaking, SQL: "Breaking change"},
			{Kind: core.OperationBreaking, SQL: "Another breaking"},
		},
		want: []core.Operation{
			{Kind: core.OperationBreaking, SQL: "Breaking change"},
			{Kind: core.OperationBreaking, SQL: "Another breaking"},
		},
	},
	{
		name: "duplicate unresolved notes are removed",
		operations: []core.Operation{
			{Kind: core.OperationUnresolved, UnresolvedReason: "Cannot resolve"},
			{Kind: core.OperationUnresolved, UnresolvedReason: "Cannot resolve"},
			{Kind: core.OperationUnresolved, UnresolvedReason: "Different issue"},
		},
		want: []core.Operation{
			{Kind: core.OperationUnresolved, UnresolvedReason: "Cannot resolve"},
			{Kind: core.OperationUnresolved, UnresolvedReason: "Different issue"},
		},
	},
	{
		name: "duplicate rollback SQL is cleared on duplicate",
		operations: []core.Operation{
			{Kind: core.OperationSQL, SQL: "CREATE TABLE users", RollbackSQL: "DROP TABLE users"},
			{Kind: core.OperationSQL, SQL: "CREATE TABLE posts", RollbackSQL: "DROP TABLE users"},
		},
		want: []core.Operation{
			{Kind: core.OperationSQL, SQL: "CREATE TABLE users", RollbackSQL: "DROP TABLE users"},
			{Kind: core.OperationSQL, SQL: "CREATE TABLE posts", RollbackSQL: ""},
		},
	},
	{
		name: "empty SQL operations are removed",
		operations: []core.Operation{
			{Kind: core.OperationSQL, SQL: "", RollbackSQL: ""},
			{Kind: core.OperationSQL, SQL: "CREATE TABLE users"},
		},
		want: []core.Operation{
			{Kind: core.OperationSQL, SQL: "CREATE TABLE users"},
		},
	},
	{
		name: "whitespace is trimmed before deduplication",
		operations: []core.Operation{
			{Kind: core.OperationNote, SQL: "  Note  "},
			{Kind: core.OperationNote, SQL: "Note"},
		},
		want: []core.Operation{
			{Kind: core.OperationNote, SQL: "Note"},
		},
	},
	{
		name: "complex mixed scenario",
		operations: []core.Operation{
			{Kind: core.OperationSQL, SQL: "CREATE TABLE users", RollbackSQL: "DROP TABLE users"},
			{Kind: core.OperationNote, SQL: "Note 1"},
			{Kind: core.OperationBreaking, SQL: "Breaking 1"},
			{Kind: core.OperationUnresolved, UnresolvedReason: "Issue 1"},
			{Kind: core.OperationSQL, SQL: "CREATE TABLE posts", RollbackSQL: "DROP TABLE users"},
			{Kind: core.OperationNote, SQL: "Note 1"},
			{Kind: core.OperationBreaking, SQL: "Breaking 1"},
			{Kind: core.OperationUnresolved, UnresolvedReason: "Issue 1"},
			{Kind: core.OperationNote, SQL: "   "},
		},
		want: []core.Operation{
			{Kind: core.OperationSQL, SQL: "CREATE TABLE users", RollbackSQL: "DROP TABLE users"},
			{Kind: core.OperationNote, SQL: "Note 1"},
			{Kind: core.OperationBreaking, SQL: "Breaking 1"},
			{Kind: core.OperationUnresolved, UnresolvedReason: "Issue 1"},
			{Kind: core.OperationSQL, SQL: "CREATE TABLE posts", RollbackSQL: ""},
		},
	},
}

func TestMigrationDedupe(t *testing.T) {
	for _, tt := range migrationDedupeTests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Migration{Operations: tt.operations}
			m.Dedupe()
			assert.Equal(t, tt.want, m.Operations)
		})
	}
}

func TestMigrationMultipleAddCalls(t *testing.T) {
	m := &Migration{}

	m.AddStatement("CREATE TABLE users (id INT)")
	m.AddStatementWithRollback("CREATE TABLE posts (id INT)", "DROP TABLE posts")
	m.AddBreaking("Schema breaking change")
	m.AddNote("Informational note")
	m.AddUnresolved("Unresolved issue")

	expectedOps := []core.Operation{
		{Kind: core.OperationSQL, SQL: "CREATE TABLE users (id INT)"},
		{Kind: core.OperationSQL, SQL: "CREATE TABLE posts (id INT)", RollbackSQL: "DROP TABLE posts"},
		{Kind: core.OperationBreaking, SQL: "Schema breaking change", Risk: core.RiskBreaking},
		{Kind: core.OperationNote, SQL: "Informational note", Risk: core.RiskInfo},
		{Kind: core.OperationUnresolved, UnresolvedReason: "Unresolved issue"},
	}

	assert.Equal(t, expectedOps, m.Operations)
	assert.Equal(t, []string{"CREATE TABLE users (id INT)", "CREATE TABLE posts (id INT)"}, m.SQLStatements())
	assert.Equal(t, []string{"DROP TABLE posts"}, m.RollbackStatements())
	assert.Equal(t, []string{"Schema breaking change"}, m.BreakingNotes())
	assert.Equal(t, []string{"Informational note"}, m.InfoNotes())
	assert.Equal(t, []string{"Unresolved issue"}, m.UnresolvedNotes())
}

func TestMigrationDedupePreservesOrder(t *testing.T) {
	m := &Migration{
		Operations: []core.Operation{
			{Kind: core.OperationSQL, SQL: "First SQL"},
			{Kind: core.OperationNote, SQL: "First Note"},
			{Kind: core.OperationSQL, SQL: "Second SQL"},
			{Kind: core.OperationNote, SQL: "First Note"},
			{Kind: core.OperationSQL, SQL: "Third SQL"},
		},
	}

	m.Dedupe()

	expected := []core.Operation{
		{Kind: core.OperationSQL, SQL: "First SQL"},
		{Kind: core.OperationNote, SQL: "First Note"},
		{Kind: core.OperationSQL, SQL: "Second SQL"},
		{Kind: core.OperationSQL, SQL: "Third SQL"},
	}

	assert.Equal(t, expected, m.Operations)
}
