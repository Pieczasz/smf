package tests

import (
	"smf/dialect/mysql"
	"smf/diff"
	"testing"

	"smf/core"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBreakingChangeAnalyzer(t *testing.T) {
	t.Run("all severities and types", func(t *testing.T) {
		oldDB := &core.Database{Tables: []*core.Table{
			{
				Name: "users",
				Columns: []*core.Column{
					{Name: "id", TypeRaw: "INT", Type: core.NormalizeDataType("INT"), Nullable: false, PrimaryKey: true, AutoIncrement: true, Charset: "utf8mb4", Collate: "utf8mb4_unicode_ci", Comment: "old"},
					{Name: "name", TypeRaw: "VARCHAR(100)", Type: core.NormalizeDataType("VARCHAR(100)"), Nullable: true, DefaultValue: strPtr("old"), Comment: "old"},
					{Name: "bio", TypeRaw: "TEXT", Type: core.NormalizeDataType("TEXT"), Nullable: true},
					{Name: "gen", TypeRaw: "INT", Type: core.NormalizeDataType("INT"), Nullable: true, IsGenerated: true, GenerationExpression: "id + 1", GenerationStorage: core.GenerationVirtual},
				},
				Constraints: []*core.Constraint{
					{Name: "PRIMARY", Type: core.ConstraintPrimaryKey, Columns: []string{"id"}},
					{Name: "uq_users_name", Type: core.ConstraintUnique, Columns: []string{"name"}},
				},
				Indexes: []*core.Index{
					{Name: "idx_name", Columns: []core.IndexColumn{{Name: "name"}}, Unique: false, Type: core.IndexTypeBTree},
				},
				Options: core.TableOptions{Engine: "InnoDB", Charset: "utf8mb4", Collate: "utf8mb4_unicode_ci"},
			},
			{
				Name:        "to_drop",
				Columns:     []*core.Column{{Name: "id", TypeRaw: "INT", Type: core.NormalizeDataType("INT"), Nullable: false, PrimaryKey: true}},
				Constraints: []*core.Constraint{{Name: "PRIMARY", Type: core.ConstraintPrimaryKey, Columns: []string{"id"}}},
			},
		}}

		newDB := &core.Database{Tables: []*core.Table{
			{
				Name: "users",
				Columns: []*core.Column{
					// type widening: INT -> BIGINT (INFO)
					{Name: "id", TypeRaw: "BIGINT", Type: core.NormalizeDataType("BIGINT"), Nullable: false, PrimaryKey: true, AutoIncrement: true, Charset: "utf8mb4", Collate: "utf8mb4_unicode_ci", Comment: "old"},
					// length shrink + NOT NULL + default change + comment change
					{Name: "name", TypeRaw: "VARCHAR(50)", Type: core.NormalizeDataType("VARCHAR(50)"), Nullable: false, DefaultValue: strPtr("new"), Comment: "new"},
					// dropped column (CRITICAL)
					// bio removed
					// generated expression change (BREAKING)
					{Name: "gen", TypeRaw: "INT", Type: core.NormalizeDataType("INT"), Nullable: true, IsGenerated: true, GenerationExpression: "id + 2", GenerationStorage: core.GenerationVirtual},
					// new NOT NULL without default (BREAKING)
					{Name: "email", TypeRaw: "VARCHAR(255)", Type: core.NormalizeDataType("VARCHAR(255)"), Nullable: false},
					// column rename: old_col -> renamed_col (reported via rename heuristic)
					{Name: "renamed_col", TypeRaw: "INT", Type: core.NormalizeDataType("INT"), Nullable: true, Comment: "same"},
				},
				Constraints: []*core.Constraint{
					// PK remains
					{Name: "PRIMARY", Type: core.ConstraintPrimaryKey, Columns: []string{"id"}},
					// modify unique constraint (WARNING)
					{Name: "uq_users_name", Type: core.ConstraintUnique, Columns: []string{"email"}},
					// add check constraint (BREAKING)
					{Name: "chk_email", Type: core.ConstraintCheck, Columns: []string{"email"}, CheckExpression: "email <> ''"},
				},
				Indexes: []*core.Index{
					// modify index columns (WARNING)
					{Name: "idx_name", Columns: []core.IndexColumn{{Name: "email"}}, Unique: false, Type: core.IndexTypeBTree},
					// add unique index (BREAKING)
					{Name: "uidx_email", Columns: []core.IndexColumn{{Name: "email"}}, Unique: true, Type: core.IndexTypeBTree},
					// add fulltext index (INFO)
					{Name: "ft_name", Columns: []core.IndexColumn{{Name: "name"}}, Unique: false, Type: core.IndexTypeFullText},
				},
				Options: core.TableOptions{Engine: "MyISAM", Charset: "latin1", Collate: "latin1_swedish_ci"},
			},
		}}

		oldUsers := oldDB.FindTable("users")
		require.NotNil(t, oldUsers)
		oldUsers.Columns = append(oldUsers.Columns, &core.Column{Name: "old_col", TypeRaw: "INT", Type: core.NormalizeDataType("INT"), Nullable: true, Comment: "same"})

		d := diff.Diff(oldDB, newDB)
		require.NotNil(t, d)

		an := diff.NewBreakingChangeAnalyzer()
		changes := an.Analyze(d)
		require.NotEmpty(t, changes)

		assert.True(t, hasBC(changes, diff.SeverityInfo, "users", "id", "type changes"))
		assert.True(t, hasBC(changes, diff.SeverityBreaking, "users", "name", "becomes NOT NULL"))
		assert.True(t, hasBC(changes, diff.SeverityBreaking, "users", "name", "length shrinks"))
		assert.True(t, hasBC(changes, diff.SeverityWarning, "users", "name", "Default value changes"))
		assert.True(t, hasBC(changes, diff.SeverityInfo, "users", "name", "comment"))
		assert.True(t, hasBC(changes, diff.SeverityCritical, "users", "bio", "Column will be dropped"))
		assert.True(t, hasBC(changes, diff.SeverityBreaking, "users", "gen", "Generated column expression changed"))
		assert.True(t, hasBC(changes, diff.SeverityBreaking, "users", "email", "Adding NOT NULL column"))
		assert.True(t, hasBC(changes, diff.SeverityBreaking, "users", "old_col->renamed_col", "Column rename detected"))
		assert.True(t, hasBC(changes, diff.SeverityWarning, "users", "idx_name", "Index modified"))
		assert.True(t, hasBC(changes, diff.SeverityBreaking, "users", "uidx_email", "Unique index added"))

		assert.True(t, hasBC(changes, diff.SeverityBreaking, "users", "ENGINE", "Storage engine changes"))
		assert.True(t, hasBC(changes, diff.SeverityWarning, "users", "CHARSET", "Character set changes"))
		assert.True(t, hasBC(changes, diff.SeverityWarning, "users", "COLLATE", "Collation changes"))

		assert.True(t, hasBC(changes, diff.SeverityCritical, "to_drop", "to_drop", "Table will be dropped"))
	})

	t.Run("type conversion safety", func(t *testing.T) {
		oldDB := &core.Database{Tables: []*core.Table{{
			Name: "t",
			Columns: []*core.Column{
				{Name: "widen", TypeRaw: "INT", Type: core.NormalizeDataType("INT"), Nullable: true},
				{Name: "narrow", TypeRaw: "BIGINT", Type: core.NormalizeDataType("BIGINT"), Nullable: true},
				{Name: "incompat", TypeRaw: "INT", Type: core.NormalizeDataType("INT"), Nullable: true},
			},
		}}}
		newDB := &core.Database{Tables: []*core.Table{{
			Name: "t",
			Columns: []*core.Column{
				{Name: "widen", TypeRaw: "BIGINT", Type: core.NormalizeDataType("BIGINT"), Nullable: true},
				{Name: "narrow", TypeRaw: "INT", Type: core.NormalizeDataType("INT"), Nullable: true},
				{Name: "incompat", TypeRaw: "VARCHAR(10)", Type: core.NormalizeDataType("VARCHAR(10)"), Nullable: true},
			},
		}}}

		d := diff.Diff(oldDB, newDB)
		an := diff.NewBreakingChangeAnalyzer()
		changes := an.Analyze(d)

		assert.True(t, hasBC(changes, diff.SeverityInfo, "t", "widen", "type changes"))
		assert.True(t, hasBC(changes, diff.SeverityCritical, "t", "narrow", "type changes"))
		assert.True(t, hasBC(changes, diff.SeverityCritical, "t", "incompat", "type changes"))
	})
}

func TestMigrationGenerationSafetyNotesAndRollback(t *testing.T) {
	oldDB := &core.Database{Tables: []*core.Table{{
		Name:        "t",
		Columns:     []*core.Column{{Name: "id", TypeRaw: "INT", Type: core.NormalizeDataType("INT"), Nullable: false, PrimaryKey: true}},
		Constraints: []*core.Constraint{{Name: "PRIMARY", Type: core.ConstraintPrimaryKey, Columns: []string{"id"}}},
		Indexes:     []*core.Index{{Name: "idx_id", Columns: []core.IndexColumn{{Name: "id"}}, Unique: false, Type: core.IndexTypeBTree}},
		Options:     core.TableOptions{Engine: "InnoDB", Charset: "utf8mb4", Collate: "utf8mb4_unicode_ci"},
	}}}

	newDB := &core.Database{Tables: []*core.Table{{
		Name: "t",
		Columns: []*core.Column{
			{Name: "id", TypeRaw: "INT", Type: core.NormalizeDataType("INT"), Nullable: false, PrimaryKey: true},
			{Name: "email", TypeRaw: "VARCHAR(255)", Type: core.NormalizeDataType("VARCHAR(255)"), Nullable: false},
		},
		Constraints: []*core.Constraint{{Name: "PRIMARY", Type: core.ConstraintPrimaryKey, Columns: []string{"id"}}},
		Indexes: []*core.Index{
			{Name: "idx_id", Columns: []core.IndexColumn{{Name: "email"}}, Unique: false, Type: core.IndexTypeBTree},
		},
		Options: core.TableOptions{Engine: "MyISAM", Charset: "latin1", Collate: "latin1_swedish_ci"},
	}}}

	d := diff.Diff(oldDB, newDB)
	require.NotNil(t, d)

	mig := mysql.NewMySQLDialect().Generator().GenerateMigration(d)
	require.NotNil(t, mig)

	out := mig.String()
	assert.Contains(t, out, "-- SQL")
	assert.Contains(t, out, "ALTER TABLE")
	assert.Contains(t, out, "Lock-time warning")
	assert.Contains(t, out, "ROLLBACK SQL")
}

func hasBC(changes []diff.BreakingChange, sev diff.ChangeSeverity, table, object, descSubstr string) bool {
	for _, c := range changes {
		if c.Severity != sev {
			continue
		}
		if c.Table != table {
			continue
		}
		if c.Object != object {
			continue
		}
		if descSubstr != "" && !containsCI(c.Description, descSubstr) {
			continue
		}
		return true
	}
	return false
}

func containsCI(s, sub string) bool {
	return len(sub) == 0 || (len(s) > 0 && (stringContainsFold(s, sub)))
}

func stringContainsFold(s, sub string) bool {
	if sub == "" {
		return true
	}
	ss := []rune(s)
	bb := []rune(sub)
	for i := 0; i+len(bb) <= len(ss); i++ {
		match := true
		for j := range bb {
			r1 := ss[i+j]
			r2 := bb[j]
			if r1 >= 'A' && r1 <= 'Z' {
				r1 = r1 - 'A' + 'a'
			}
			if r2 >= 'A' && r2 <= 'Z' {
				r2 = r2 - 'A' + 'a'
			}
			if r1 != r2 {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}

func strPtr(s string) *string {
	return &s
}
