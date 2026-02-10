package mysql

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"smf/internal/core"
	"smf/internal/dialect"
	"smf/internal/diff"
)

func TestDialectName(t *testing.T) {
	d := NewMySQLDialect()
	assert.Equal(t, dialect.MySQL, d.Name())
}

func TestDialectGenerator(t *testing.T) {
	d := NewMySQLDialect()
	gen := d.Generator()
	require.NotNil(t, gen)
	assert.IsType(t, &Generator{}, gen)
}

func TestDialectParser(t *testing.T) {
	d := NewMySQLDialect()
	p := d.Parser()
	require.NotNil(t, p)
}

func TestGeneratorGenerateAlterTable(t *testing.T) {
	g := NewMySQLGenerator()

	td := &diff.TableDiff{
		Name: "users",
		AddedColumns: []*core.Column{
			{Name: "email", TypeRaw: "VARCHAR(255)", Nullable: true},
		},
	}

	stmts := g.GenerateAlterTable(td)

	require.NotEmpty(t, stmts)
	assert.Contains(t, stmts[0], "ALTER TABLE")
	assert.Contains(t, stmts[0], "ADD COLUMN")
	assert.Contains(t, stmts[0], "`email`")
}

func TestGeneratorGenerateAlterTableEmpty(t *testing.T) {
	g := NewMySQLGenerator()

	td := &diff.TableDiff{
		Name: "users",
	}

	stmts := g.GenerateAlterTable(td)
	assert.Empty(t, stmts)
}

func TestGeneratorGenerateAlterTableMultipleChanges(t *testing.T) {
	g := NewMySQLGenerator()

	td := &diff.TableDiff{
		Name: "users",
		AddedColumns: []*core.Column{
			{Name: "email", TypeRaw: "VARCHAR(255)", Nullable: true},
			{Name: "phone", TypeRaw: "VARCHAR(20)", Nullable: true},
		},
		ModifiedColumns: []*diff.ColumnChange{
			{
				Old: &core.Column{Name: "name", TypeRaw: "VARCHAR(100)", Nullable: true},
				New: &core.Column{Name: "name", TypeRaw: "VARCHAR(255)", Nullable: false},
			},
		},
	}

	stmts := g.GenerateAlterTable(td)

	require.GreaterOrEqual(t, len(stmts), 3)
}

func TestGeneratorGenerateAlterTableWithIndexes(t *testing.T) {
	g := NewMySQLGenerator()

	td := &diff.TableDiff{
		Name: "users",
		AddedIndexes: []*core.Index{
			{Name: "idx_email", Columns: []core.IndexColumn{{Name: "email"}}},
		},
		RemovedIndexes: []*core.Index{
			{Name: "idx_old", Columns: []core.IndexColumn{{Name: "old_col"}}},
		},
	}

	stmts := g.GenerateAlterTable(td)

	require.GreaterOrEqual(t, len(stmts), 2)

	hasDropIndex := false
	hasCreateIndex := false
	for _, stmt := range stmts {
		if strings.Contains(stmt, "DROP INDEX") && strings.Contains(stmt, "`idx_old`") {
			hasDropIndex = true
		}
		if strings.Contains(stmt, "CREATE INDEX") && strings.Contains(stmt, "`idx_email`") {
			hasCreateIndex = true
		}
	}
	assert.True(t, hasDropIndex, "should have DROP INDEX statement")
	assert.True(t, hasCreateIndex, "should have CREATE INDEX statement")
}

func TestGeneratorGenerateAlterTableWithConstraints(t *testing.T) {
	g := NewMySQLGenerator()

	td := &diff.TableDiff{
		Name: "users",
		AddedConstraints: []*core.Constraint{
			{Name: "uq_email", Type: core.ConstraintUnique, Columns: []string{"email"}},
		},
		RemovedConstraints: []*core.Constraint{
			{Name: "uq_old", Type: core.ConstraintUnique, Columns: []string{"old_col"}},
		},
	}

	stmts := g.GenerateAlterTable(td)

	require.GreaterOrEqual(t, len(stmts), 2)
}

func TestGeneratorGenerateMigration(t *testing.T) {
	g := NewMySQLGenerator()

	schemaDiff := &diff.SchemaDiff{
		AddedTables: []*core.Table{
			{
				Name: "users",
				Columns: []*core.Column{
					{Name: "id", TypeRaw: "INT", Nullable: false, AutoIncrement: true},
					{Name: "name", TypeRaw: "VARCHAR(255)", Nullable: true},
				},
			},
		},
	}

	mig := g.GenerateMigration(schemaDiff, dialect.DefaultMigrationOptions(dialect.MySQL))

	require.NotNil(t, mig)
	assert.NotEmpty(t, mig.Plan())
}

func TestGeneratorGenerateMigrationWithOptions(t *testing.T) {
	g := NewMySQLGenerator()

	schemaDiff := &diff.SchemaDiff{
		RemovedTables: []*core.Table{
			{Name: "old_table"},
		},
	}

	opts := dialect.MigrationOptions{
		Dialect:       dialect.MySQL,
		IncludeDrops:  true,
		IncludeUnsafe: true,
	}

	mig := g.GenerateMigration(schemaDiff, opts)

	require.NotNil(t, mig)
	plan := mig.Plan()

	hasDropTable := false
	for _, op := range plan {
		if op.Kind == core.OperationSQL && strings.Contains(op.SQL, "DROP TABLE") {
			hasDropTable = true
			break
		}
	}
	assert.True(t, hasDropTable, "should have DROP TABLE statement in unsafe mode")
}

func TestGeneratorGenerateMigrationSafeMode(t *testing.T) {
	g := NewMySQLGenerator()

	schemaDiff := &diff.SchemaDiff{
		RemovedTables: []*core.Table{
			{Name: "old_table"},
		},
	}

	mig := g.GenerateMigration(schemaDiff, dialect.DefaultMigrationOptions(dialect.MySQL))

	require.NotNil(t, mig)
	plan := mig.Plan()

	hasRename := false
	for _, op := range plan {
		if op.Kind == core.OperationSQL && strings.Contains(op.SQL, "RENAME TABLE") {
			hasRename = true
			break
		}
	}
	assert.True(t, hasRename, "safe mode should rename instead of drop")
}

func TestGeneratorGenerateCreateTable(t *testing.T) {
	g := NewMySQLGenerator()

	table := &core.Table{
		Name: "users",
		Columns: []*core.Column{
			{Name: "id", TypeRaw: "INT", Nullable: false, AutoIncrement: true},
			{Name: "name", TypeRaw: "VARCHAR(255)", Nullable: true},
		},
		Constraints: []*core.Constraint{
			{Type: core.ConstraintPrimaryKey, Columns: []string{"id"}},
		},
	}

	stmt, fks := g.GenerateCreateTable(table)

	assert.Contains(t, stmt, "CREATE TABLE `users`")
	assert.Contains(t, stmt, "`id`")
	assert.Contains(t, stmt, "`name`")
	assert.Contains(t, stmt, "PRIMARY KEY")
	assert.Empty(t, fks)
}

func TestGeneratorGenerateCreateTableWithFK(t *testing.T) {
	g := NewMySQLGenerator()

	table := &core.Table{
		Name: "orders",
		Columns: []*core.Column{
			{Name: "id", TypeRaw: "INT", Nullable: false},
			{Name: "user_id", TypeRaw: "INT", Nullable: false},
		},
		Constraints: []*core.Constraint{
			{Type: core.ConstraintPrimaryKey, Columns: []string{"id"}},
			{
				Name:              "fk_user",
				Type:              core.ConstraintForeignKey,
				Columns:           []string{"user_id"},
				ReferencedTable:   "users",
				ReferencedColumns: []string{"id"},
				OnDelete:          "CASCADE",
				OnUpdate:          "NO ACTION",
			},
		},
	}

	stmt, fks := g.GenerateCreateTable(table)

	assert.Contains(t, stmt, "CREATE TABLE `orders`")
	assert.NotContains(t, stmt, "FOREIGN KEY")
	require.Len(t, fks, 1)
	assert.Contains(t, fks[0], "FOREIGN KEY")
	assert.Contains(t, fks[0], "REFERENCES `users`")
}

func TestGeneratorGenerateDropTable(t *testing.T) {
	g := NewMySQLGenerator()

	table := &core.Table{Name: "users"}

	stmt := g.GenerateDropTable(table)

	assert.Equal(t, "DROP TABLE `users`;", stmt)
}

func TestSafeBackupName(t *testing.T) {
	g := NewMySQLGenerator()

	tests := []struct {
		name     string
		input    string
		contains string
	}{
		{"simple name", "users", "__smf_backup_"},
		{"with spaces", " users ", "__smf_backup_"},
		{"long name", "this_is_a_very_long_table_name_that_exceeds_mysql_limit", "__smf_backup_"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := g.safeBackupName(tt.input)
			assert.Contains(t, result, tt.contains)
			assert.LessOrEqual(t, len(result), 64)
		})
	}
}

func TestSafeBackupNameEmpty(t *testing.T) {
	g := NewMySQLGenerator()

	result := g.safeBackupName("")
	assert.Contains(t, result, "__smf_backup_")
}

func TestHasPotentiallyLockingStatements(t *testing.T) {
	tests := []struct {
		name     string
		plan     []core.Operation
		expected bool
	}{
		{
			"with ALTER TABLE",
			[]core.Operation{{Kind: core.OperationSQL, SQL: "ALTER TABLE users ADD COLUMN x INT;"}},
			true,
		},
		{
			"with CREATE INDEX",
			[]core.Operation{{Kind: core.OperationSQL, SQL: "CREATE INDEX idx ON users(x);"}},
			true,
		},
		{
			"with DROP INDEX",
			[]core.Operation{{Kind: core.OperationSQL, SQL: "DROP INDEX idx ON users;"}},
			true,
		},
		{
			"with SELECT",
			[]core.Operation{{Kind: core.OperationSQL, SQL: "SELECT * FROM users;"}},
			false,
		},
		{
			"empty plan",
			[]core.Operation{},
			false,
		},
		{
			"non-SQL operation",
			[]core.Operation{{Kind: core.OperationNote, SQL: ""}},
			false,
		},
		{
			"lowercase alter table",
			[]core.Operation{{Kind: core.OperationSQL, SQL: "alter table users add column x int;"}},
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := hasPotentiallyLockingStatements(tt.plan)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestHasPrefixFoldCI(t *testing.T) {
	tests := []struct {
		s        string
		prefix   string
		expected bool
	}{
		{"ALTER TABLE users", "ALTER TABLE", true},
		{"alter table users", "ALTER TABLE", true},
		{"ALTER TABLE", "ALTER TABLE", true},
		{"SELECT * FROM users", "ALTER TABLE", false},
		{"ALT", "ALTER TABLE", false},
		{"", "ALTER TABLE", false},
	}

	for _, tt := range tests {
		t.Run(tt.s, func(t *testing.T) {
			result := hasPrefixFoldCI(tt.s, tt.prefix)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGenerateCreateTableWithNilColumn(t *testing.T) {
	g := NewMySQLGenerator()

	table := &core.Table{
		Name: "users",
		Columns: []*core.Column{
			{Name: "id", TypeRaw: "INT", Nullable: false},
			nil,
			{Name: "name", TypeRaw: "VARCHAR(255)", Nullable: true},
		},
	}

	stmt, _ := g.GenerateCreateTable(table)

	assert.Contains(t, stmt, "`id`")
	assert.Contains(t, stmt, "`name`")
}

func TestGenerateCreateTableWithNilConstraint(t *testing.T) {
	g := NewMySQLGenerator()

	table := &core.Table{
		Name: "users",
		Columns: []*core.Column{
			{Name: "id", TypeRaw: "INT", Nullable: false},
		},
		Constraints: []*core.Constraint{
			nil,
			{Type: core.ConstraintPrimaryKey, Columns: []string{"id"}},
		},
	}

	stmt, _ := g.GenerateCreateTable(table)

	assert.Contains(t, stmt, "PRIMARY KEY")
}

func TestGenerateCreateTableWithNilIndex(t *testing.T) {
	g := NewMySQLGenerator()

	table := &core.Table{
		Name: "users",
		Columns: []*core.Column{
			{Name: "id", TypeRaw: "INT", Nullable: false},
			{Name: "email", TypeRaw: "VARCHAR(255)", Nullable: true},
		},
		Indexes: []*core.Index{
			nil,
			{Name: "idx_email", Columns: []core.IndexColumn{{Name: "email"}}},
		},
	}

	stmt, _ := g.GenerateCreateTable(table)

	assert.Contains(t, stmt, "KEY `idx_email`")
}

func TestGenerateCreateTableWithIndexNoName(t *testing.T) {
	g := NewMySQLGenerator()

	table := &core.Table{
		Name: "users",
		Columns: []*core.Column{
			{Name: "id", TypeRaw: "INT", Nullable: false},
		},
		Indexes: []*core.Index{
			{Name: "", Columns: []core.IndexColumn{{Name: "id"}}},
		},
	}

	stmt, _ := g.GenerateCreateTable(table)

	assert.NotContains(t, stmt, "KEY ``")
}

func TestGenerateMigrationWithPendingFKs(t *testing.T) {
	g := NewMySQLGenerator()

	schemaDiff := &diff.SchemaDiff{
		AddedTables: []*core.Table{
			{
				Name: "orders",
				Columns: []*core.Column{
					{Name: "id", TypeRaw: "INT", Nullable: false},
					{Name: "user_id", TypeRaw: "INT", Nullable: false},
				},
				Constraints: []*core.Constraint{
					{Type: core.ConstraintPrimaryKey, Columns: []string{"id"}},
					{
						Name:              "fk_user",
						Type:              core.ConstraintForeignKey,
						Columns:           []string{"user_id"},
						ReferencedTable:   "users",
						ReferencedColumns: []string{"id"},
					},
				},
			},
		},
	}

	mig := g.GenerateMigration(schemaDiff, dialect.DefaultMigrationOptions(dialect.MySQL))

	require.NotNil(t, mig)
	plan := mig.Plan()

	hasFKNote := false
	for _, op := range plan {
		if op.Kind == core.OperationNote && strings.Contains(op.SQL, "Foreign keys added") {
			hasFKNote = true
			break
		}
	}
	assert.True(t, hasFKNote)
}

func TestHasPotentiallyLockingStatementsEmptySQL(t *testing.T) {
	plan := []core.Operation{
		{Kind: core.OperationSQL, SQL: ""},
		{Kind: core.OperationSQL, SQL: "   "},
	}

	result := hasPotentiallyLockingStatements(plan)
	assert.False(t, result)
}

func TestGenerateMigrationWithModifiedTableMismatchedRollback(t *testing.T) {
	g := NewMySQLGenerator()

	schemaDiff := &diff.SchemaDiff{
		ModifiedTables: []*diff.TableDiff{
			{
				Name: "users",
				AddedIndexes: []*core.Index{
					{Name: "", Columns: []core.IndexColumn{{Name: "email"}}},
				},
			},
		},
	}

	mig := g.GenerateMigration(schemaDiff, dialect.DefaultMigrationOptions(dialect.MySQL))
	require.NotNil(t, mig)
}

func TestGenerateMigrationWithFKStatementWithoutRollback(t *testing.T) {
	g := NewMySQLGenerator()

	schemaDiff := &diff.SchemaDiff{
		ModifiedTables: []*diff.TableDiff{
			{
				Name: "orders",
				AddedConstraints: []*core.Constraint{
					{
						Name:              "fk_user",
						Type:              core.ConstraintForeignKey,
						Columns:           []string{"user_id"},
						ReferencedTable:   "users",
						ReferencedColumns: []string{"id"},
					},
				},
			},
		},
	}

	mig := g.GenerateMigration(schemaDiff, dialect.DefaultMigrationOptions(dialect.MySQL))
	require.NotNil(t, mig)

	plan := mig.Plan()
	hasFKStatement := false
	for _, op := range plan {
		if op.Kind == core.OperationSQL && strings.Contains(op.SQL, "FOREIGN KEY") {
			hasFKStatement = true
			break
		}
	}
	assert.True(t, hasFKStatement)
}

func TestGenerateMigrationWithFKNoRollbackBranch(t *testing.T) {
	g := NewMySQLGenerator()

	schemaDiff := &diff.SchemaDiff{
		AddedTables: []*core.Table{
			{
				Name: "orders",
				Columns: []*core.Column{
					{Name: "id", TypeRaw: "INT", Nullable: false},
					{Name: "user_id", TypeRaw: "INT", Nullable: false},
				},
				Constraints: []*core.Constraint{
					{Type: core.ConstraintPrimaryKey, Columns: []string{"id"}},
					{
						// Unnamed FK - dropConstraint will return a comment, not empty
						Name:              "",
						Type:              core.ConstraintForeignKey,
						Columns:           []string{"user_id"},
						ReferencedTable:   "users",
						ReferencedColumns: []string{"id"},
					},
				},
			},
		},
	}

	mig := g.GenerateMigration(schemaDiff, dialect.DefaultMigrationOptions(dialect.MySQL))
	require.NotNil(t, mig)
}

func TestGenerateMigrationWithOrphanedFKRollbacks(t *testing.T) {
	g := NewMySQLGenerator()

	schemaDiff := &diff.SchemaDiff{
		AddedTables: []*core.Table{
			{
				Name: "orders",
				Columns: []*core.Column{
					{Name: "id", TypeRaw: "INT", Nullable: false},
					{Name: "user_id", TypeRaw: "INT", Nullable: false},
				},
				Constraints: []*core.Constraint{
					{
						Name:              "fk_valid",
						Type:              core.ConstraintForeignKey,
						Columns:           []string{"user_id"},
						ReferencedTable:   "users",
						ReferencedColumns: []string{"id"},
					},
				},
			},
		},
	}

	mig := g.GenerateMigration(schemaDiff, dialect.DefaultMigrationOptions(dialect.MySQL))
	require.NotNil(t, mig)
}
