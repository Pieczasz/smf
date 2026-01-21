package mysql

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"smf/internal/core"
	"smf/internal/diff"
)

func TestMySQLGeneratorDoesNotEmitCharsetCollateForJSONAndBinary(t *testing.T) {
	oldDB := &core.Database{Tables: []*core.Table{{
		Name:        "t",
		Columns:     []*core.Column{{Name: "id", TypeRaw: "INT", Type: core.NormalizeDataType("INT"), Nullable: false, PrimaryKey: true, AutoIncrement: true}},
		Constraints: []*core.Constraint{{Name: "PRIMARY", Type: core.ConstraintPrimaryKey, Columns: []string{"id"}}},
	}}}

	newDB := &core.Database{Tables: []*core.Table{{
		Name: "t",
		Columns: []*core.Column{
			{Name: "id", TypeRaw: "INT", Type: core.NormalizeDataType("INT"), Nullable: false, PrimaryKey: true, AutoIncrement: true},
			{Name: "payload", TypeRaw: "json", Type: core.NormalizeDataType("json"), Nullable: true, Charset: "binary", Collate: "binary"},
			{Name: "uuid", TypeRaw: "binary(16)", Type: core.NormalizeDataType("binary(16)"), Nullable: false, Charset: "binary", Collate: "binary"},
		},
		Constraints: []*core.Constraint{{Name: "PRIMARY", Type: core.ConstraintPrimaryKey, Columns: []string{"id"}}},
	}}}

	d := diff.Diff(oldDB, newDB, diff.DefaultOptions())
	require.NotNil(t, d)

	mig := NewMySQLDialect().Generator().GenerateMigration(d)
	out := mig.String()

	assert.Contains(t, out, "ALTER TABLE `t` ADD COLUMN `payload` json")
	assert.Contains(t, out, "ALTER TABLE `t` ADD COLUMN `uuid` binary(16)")
	assert.NotContains(t, out, "`payload` json NULL CHARACTER SET")
	assert.NotContains(t, out, "`payload` json NULL COLLATE")
	assert.NotContains(t, out, "`uuid` binary(16) NOT NULL CHARACTER SET")
	assert.NotContains(t, out, "`uuid` binary(16) NOT NULL COLLATE")
}

func TestMySQLGeneratorDoesNotEmitBinaryAttributeForVarbinary(t *testing.T) {
	oldDB := &core.Database{Tables: []*core.Table{{
		Name:        "t",
		Columns:     []*core.Column{{Name: "id", TypeRaw: "INT", Type: core.NormalizeDataType("INT"), Nullable: false, PrimaryKey: true, AutoIncrement: true}},
		Constraints: []*core.Constraint{{Name: "PRIMARY", Type: core.ConstraintPrimaryKey, Columns: []string{"id"}}},
	}}}

	newDB := &core.Database{Tables: []*core.Table{{
		Name: "t",
		Columns: []*core.Column{
			{Name: "id", TypeRaw: "INT", Type: core.NormalizeDataType("INT"), Nullable: false, PrimaryKey: true, AutoIncrement: true},
			{Name: "v", TypeRaw: "varbinary(72) BINARY", Type: core.NormalizeDataType("varbinary(72)"), Nullable: false},
		},
		Constraints: []*core.Constraint{{Name: "PRIMARY", Type: core.ConstraintPrimaryKey, Columns: []string{"id"}}},
	}}}

	d := diff.Diff(oldDB, newDB, diff.DefaultOptions())
	require.NotNil(t, d)

	mig := NewMySQLDialect().Generator().GenerateMigration(d)
	out := mig.String()

	assert.Contains(t, out, "ALTER TABLE `t` ADD COLUMN `v` varbinary(72) NOT NULL")
	assert.NotContains(t, out, "varbinary(72) BINARY")
}

func TestMySQLGeneratorDefersFKAddsUntilEnd(t *testing.T) {
	oldDB := &core.Database{Tables: []*core.Table{
		{
			Name:        "users",
			Columns:     []*core.Column{{Name: "id", TypeRaw: "BIGINT UNSIGNED", Type: core.NormalizeDataType("BIGINT"), Nullable: false, PrimaryKey: true, AutoIncrement: true}},
			Constraints: []*core.Constraint{{Name: "PRIMARY", Type: core.ConstraintPrimaryKey, Columns: []string{"id"}}},
		},
		{
			Name: "orders",
			Columns: []*core.Column{
				{Name: "id", TypeRaw: "BIGINT UNSIGNED", Type: core.NormalizeDataType("BIGINT"), Nullable: false, PrimaryKey: true, AutoIncrement: true},
				{Name: "user_id", TypeRaw: "BIGINT UNSIGNED", Type: core.NormalizeDataType("BIGINT"), Nullable: false},
			},
			Constraints: []*core.Constraint{
				{Name: "PRIMARY", Type: core.ConstraintPrimaryKey, Columns: []string{"id"}},
				{Name: "fk_orders_user", Type: core.ConstraintForeignKey, Columns: []string{"user_id"}, ReferencedTable: "users", ReferencedColumns: []string{"id"}, OnDelete: core.RefActionRestrict, OnUpdate: core.RefActionRestrict},
			},
		},
	}}

	newDB := &core.Database{Tables: []*core.Table{
		{
			Name:        "users",
			Columns:     []*core.Column{{Name: "id", TypeRaw: "BINARY(16)", Type: core.NormalizeDataType("BINARY(16)"), Nullable: false, PrimaryKey: true}},
			Constraints: []*core.Constraint{{Name: "PRIMARY", Type: core.ConstraintPrimaryKey, Columns: []string{"id"}}},
		},
		{
			Name: "orders",
			Columns: []*core.Column{
				{Name: "id", TypeRaw: "BIGINT UNSIGNED", Type: core.NormalizeDataType("BIGINT"), Nullable: false, PrimaryKey: true, AutoIncrement: true},
				{Name: "user_id", TypeRaw: "BINARY(16)", Type: core.NormalizeDataType("BINARY(16)"), Nullable: false},
			},
			Constraints: []*core.Constraint{
				{Name: "PRIMARY", Type: core.ConstraintPrimaryKey, Columns: []string{"id"}},
				{Name: "fk_orders_user", Type: core.ConstraintForeignKey, Columns: []string{"user_id"}, ReferencedTable: "users", ReferencedColumns: []string{"id"}, OnDelete: core.RefActionCascade, OnUpdate: core.RefActionRestrict},
			},
		},
	}}

	d := diff.Diff(oldDB, newDB, diff.DefaultOptions())
	require.NotNil(t, d)

	mig := NewMySQLDialect().Generator().GenerateMigration(d)
	out := mig.String()
	sqlStart := strings.Index(out, "-- SQL\n")
	require.Greater(t, sqlStart, -1)
	sql := out[sqlStart:]

	dropFK := "ALTER TABLE `orders` DROP FOREIGN KEY `fk_orders_user`"
	addFK := "ALTER TABLE `orders` ADD CONSTRAINT `fk_orders_user` FOREIGN KEY"
	modifyOrders := "ALTER TABLE `orders` MODIFY COLUMN `user_id`"
	modifyUsers := "ALTER TABLE `users` MODIFY COLUMN `id`"

	idxDrop := strings.Index(sql, dropFK)
	idxAdd := strings.Index(sql, addFK)
	idxModOrders := strings.Index(sql, modifyOrders)
	idxModUsers := strings.Index(sql, modifyUsers)

	require.Greater(t, idxDrop, -1)
	require.Greater(t, idxAdd, -1)
	require.Greater(t, idxModOrders, -1)
	require.Greater(t, idxModUsers, -1)

	assert.Less(t, idxDrop, idxModOrders)
	assert.Less(t, idxModOrders, idxAdd)
	assert.Less(t, idxModUsers, idxAdd)
}

func TestMySQLGeneratorRebuildsUnchangedFKWhenColumnModifiedWithoutConstraintModifiedWarning(t *testing.T) {
	oldDB := &core.Database{Tables: []*core.Table{
		{
			Name:        "users",
			Columns:     []*core.Column{{Name: "id", TypeRaw: "BIGINT UNSIGNED", Type: core.NormalizeDataType("BIGINT"), Nullable: false, PrimaryKey: true, AutoIncrement: true}},
			Constraints: []*core.Constraint{{Name: "PRIMARY", Type: core.ConstraintPrimaryKey, Columns: []string{"id"}}},
		},
		{
			Name: "user_roles",
			Columns: []*core.Column{
				{Name: "user_id", TypeRaw: "BIGINT UNSIGNED", Type: core.NormalizeDataType("BIGINT"), Nullable: false},
				{Name: "role_id", TypeRaw: "BIGINT UNSIGNED", Type: core.NormalizeDataType("BIGINT"), Nullable: false},
			},
			Constraints: []*core.Constraint{
				{Name: "PRIMARY", Type: core.ConstraintPrimaryKey, Columns: []string{"user_id", "role_id"}},
				{Name: "fk_user_roles_user", Type: core.ConstraintForeignKey, Columns: []string{"user_id"}, ReferencedTable: "users", ReferencedColumns: []string{"id"}, OnDelete: core.RefActionCascade, OnUpdate: core.RefActionRestrict},
			},
		},
	}}

	newDB := &core.Database{Tables: []*core.Table{
		{
			Name:        "users",
			Columns:     []*core.Column{{Name: "id", TypeRaw: "BINARY(16)", Type: core.NormalizeDataType("BINARY(16)"), Nullable: false, PrimaryKey: true}},
			Constraints: []*core.Constraint{{Name: "PRIMARY", Type: core.ConstraintPrimaryKey, Columns: []string{"id"}}},
		},
		{
			Name: "user_roles",
			Columns: []*core.Column{
				{Name: "user_id", TypeRaw: "BINARY(16)", Type: core.NormalizeDataType("BINARY(16)"), Nullable: false},
				{Name: "role_id", TypeRaw: "BIGINT UNSIGNED", Type: core.NormalizeDataType("BIGINT"), Nullable: false},
			},
			Constraints: []*core.Constraint{
				{Name: "PRIMARY", Type: core.ConstraintPrimaryKey, Columns: []string{"user_id", "role_id"}},
				{Name: "fk_user_roles_user", Type: core.ConstraintForeignKey, Columns: []string{"user_id"}, ReferencedTable: "users", ReferencedColumns: []string{"id"}, OnDelete: core.RefActionCascade, OnUpdate: core.RefActionRestrict},
			},
		},
	}}

	d := diff.Diff(oldDB, newDB, diff.DefaultOptions())
	require.NotNil(t, d)

	mig := NewMySQLDialect().Generator().GenerateMigration(d)
	out := mig.String()
	sqlStart := strings.Index(out, "-- SQL\n")
	require.Greater(t, sqlStart, -1)
	sql := out[sqlStart:]

	assert.Contains(t, sql, "ALTER TABLE `user_roles` DROP FOREIGN KEY `fk_user_roles_user`;")
	assert.Contains(t, sql, "ALTER TABLE `user_roles` MODIFY COLUMN `user_id` BINARY(16)")
	assert.Contains(t, sql, "ALTER TABLE `user_roles` ADD CONSTRAINT `fk_user_roles_user` FOREIGN KEY")
	assert.NotContains(t, out, "Constraint modified")
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

	d := diff.Diff(oldDB, newDB, diff.DefaultOptions())
	require.NotNil(t, d)

	mig := NewMySQLDialect().Generator().GenerateMigration(d)
	require.NotNil(t, mig)

	out := mig.String()
	assert.Contains(t, out, "-- SQL")
	assert.Contains(t, out, "ALTER TABLE")
	assert.Contains(t, out, "Lock-time warning")
	assert.Contains(t, out, "ROLLBACK SQL")
}

func TestBreakingChangesVarcharLengthChangeDoesNotAlsoReportTypeChange(t *testing.T) {
	oldDB := &core.Database{Tables: []*core.Table{{
		Name:    "t",
		Columns: []*core.Column{{Name: "s", TypeRaw: "VARCHAR(32)", Type: core.NormalizeDataType("VARCHAR(32)"), Nullable: false}},
	}}}
	newDB := &core.Database{Tables: []*core.Table{{
		Name:    "t",
		Columns: []*core.Column{{Name: "s", TypeRaw: "VARCHAR(40)", Type: core.NormalizeDataType("VARCHAR(40)"), Nullable: false}},
	}}}

	d := diff.Diff(oldDB, newDB, diff.DefaultOptions())
	require.NotNil(t, d)

	changes := diff.NewBreakingChangeAnalyzer().Analyze(d)
	assert.False(t, hasBC(changes, diff.SeverityInfo, "t", "s", "type changes"))
	assert.True(t, hasBC(changes, diff.SeverityInfo, "t", "s", "length"))
}

// hasBC is a test helper to check if a breaking change exists
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
		if descSubstr != "" && !strings.Contains(strings.ToLower(c.Description), strings.ToLower(descSubstr)) {
			continue
		}
		return true
	}
	return false
}
