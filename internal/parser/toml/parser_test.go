package toml

import (
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"smf/internal/core"
)

func testdataPath(file string) string {
	_, filename, _, _ := runtime.Caller(0)
	dir := filepath.Dir(filename)
	return filepath.Join(dir, "..", "..", "..", "test", "data", file)
}

func TestParseFileSchemaToml(t *testing.T) {
	p := NewParser()
	db, err := p.ParseFile(testdataPath("schema.toml"))
	require.NoError(t, err)
	require.NotNil(t, db)

	assert.Equal(t, "ecommerce", db.Name)
	require.NotNil(t, db.Dialect)
	assert.Equal(t, core.DialectMySQL, *db.Dialect)
	assert.Len(t, db.Tables, 4)

	want := []string{"tenants", "users", "roles", "user_roles"}
	for i, name := range want {
		assert.Equal(t, name, db.Tables[i].Name)
	}

	// Validation rules parsed.
	require.NotNil(t, db.Validation)
	assert.Equal(t, 64, db.Validation.MaxTableNameLength)
	assert.Equal(t, 64, db.Validation.MaxColumnNameLength)
	assert.True(t, db.Validation.AutoGenerateConstraintNames)
	assert.Equal(t, "^[a-z][a-z0-9_]*$", db.Validation.AllowedNamePattern)
}

func TestParseFileTenants(t *testing.T) {
	p := NewParser()
	db, err := p.ParseFile(testdataPath("schema.toml"))
	require.NoError(t, err)

	tbl := db.FindTable("tenants")
	require.NotNil(t, tbl)

	assert.Equal(t, "Tenant / account", tbl.Comment)
	require.NotNil(t, tbl.Options.MySQL)
	assert.Equal(t, "InnoDB", tbl.Options.MySQL.Engine)
	assert.Equal(t, "utf8mb4", tbl.Options.MySQL.Charset)
	assert.Equal(t, "utf8mb4_unicode_ci", tbl.Options.MySQL.Collate)

	// Timestamps enabled - created_at and updated_at injected.
	require.NotNil(t, tbl.Timestamps)
	assert.True(t, tbl.Timestamps.Enabled)

	// 5 declared columns + 2 injected by timestamps = 7
	assert.Len(t, tbl.Columns, 7)

	testTenantColumns(t, tbl)
	testTenantConstraints(t, tbl)
}

func testTenantColumns(t *testing.T, tbl *core.Table) {
	t.Helper()
	id := tbl.FindColumn("id")
	require.NotNil(t, id)
	assert.Empty(t, id.RawType)
	assert.Equal(t, core.DataTypeInt, id.Type)
	assert.True(t, id.PrimaryKey)
	assert.True(t, id.AutoIncrement)
	assert.False(t, id.Nullable)

	slug := tbl.FindColumn("slug")
	require.NotNil(t, slug)
	assert.Empty(t, slug.RawType)
	assert.Equal(t, core.DataTypeString, slug.Type)
	assert.True(t, slug.Unique, "slug should have inline unique = true")

	name := tbl.FindColumn("name")
	require.NotNil(t, name)
	assert.Empty(t, name.RawType)

	plan := tbl.FindColumn("plan")
	require.NotNil(t, plan)
	// v2: type = "enum" + values = [...] -> RawType is empty (handled by generator default)
	assert.Empty(t, plan.RawType)
	assert.Equal(t, core.DataTypeEnum, plan.Type)
	assert.Equal(t, []string{"free", "pro", "enterprise"}, plan.EnumValues)
	require.NotNil(t, plan.DefaultValue)
	assert.Equal(t, "free", *plan.DefaultValue)

	settings := tbl.FindColumn("settings")
	require.NotNil(t, settings)
	assert.Empty(t, settings.RawType)
	assert.Equal(t, core.DataTypeJSON, settings.Type)
	assert.True(t, settings.Nullable)

	// Timestamps injected columns.
	createdAt := tbl.FindColumn("created_at")
	require.NotNil(t, createdAt)
	assert.Equal(t, "timestamp", createdAt.RawType)
	assert.Equal(t, core.DataTypeDatetime, createdAt.Type)
	require.NotNil(t, createdAt.DefaultValue)
	assert.Equal(t, "CURRENT_TIMESTAMP", *createdAt.DefaultValue)

	updatedAt := tbl.FindColumn("updated_at")
	require.NotNil(t, updatedAt)
	assert.Equal(t, "timestamp", updatedAt.RawType)
	require.NotNil(t, updatedAt.DefaultValue)
	assert.Equal(t, "CURRENT_TIMESTAMP", *updatedAt.DefaultValue)
	require.NotNil(t, updatedAt.OnUpdate)
	assert.Equal(t, "CURRENT_TIMESTAMP", *updatedAt.OnUpdate)
}

func testTenantConstraints(t *testing.T, tbl *core.Table) {
	t.Helper()
	// PK from column-level primary_key = true.
	pk := tbl.PrimaryKey()
	require.NotNil(t, pk)
	assert.Equal(t, core.ConstraintPrimaryKey, pk.Type)
	assert.Equal(t, []string{"id"}, pk.Columns)

	// UNIQUE from column-level unique = true on slug.
	var uqSlug *core.Constraint
	for _, c := range tbl.Constraints {
		if c.Type == core.ConstraintUnique {
			for _, col := range c.Columns {
				if col == "slug" {
					uqSlug = c
				}
			}
		}
	}
	require.NotNil(t, uqSlug, "expected auto-synthesized UNIQUE constraint for slug")
	assert.Equal(t, []string{"slug"}, uqSlug.Columns)
}

func TestParseFileUsers(t *testing.T) {
	p := NewParser()
	db, err := p.ParseFile(testdataPath("schema.toml"))
	require.NoError(t, err)

	tbl := db.FindTable("users")
	require.NotNil(t, tbl)

	assert.Equal(t, "Application user", tbl.Comment)

	// Timestamps enabled.
	require.NotNil(t, tbl.Timestamps)
	assert.True(t, tbl.Timestamps.Enabled)

	// 6 declared + 2 timestamps = 8
	assert.Len(t, tbl.Columns, 8)

	testUsersInlineFK(t, tbl)
	testUsersInlineUniqueAndCheck(t, tbl)
	testUsersBooleanDefault(t, tbl)
	testUsersPasswordHash(t, tbl)
	testUsersNullableDisplayName(t, tbl)
	testUsersSimpleIndex(t, tbl)
}

func testUsersInlineFK(t *testing.T, tbl *core.Table) {
	t.Helper()
	tenantID := tbl.FindColumn("tenant_id")
	require.NotNil(t, tenantID)
	assert.Equal(t, "tenants.id", tenantID.References)
	assert.Equal(t, core.RefActionCascade, tenantID.RefOnDelete)
	assert.Equal(t, core.RefActionRestrict, tenantID.RefOnUpdate)

	// Auto-synthesized FK constraint.
	var fk *core.Constraint
	for _, c := range tbl.Constraints {
		if c.Type == core.ConstraintForeignKey && c.ReferencedTable == "tenants" {
			fk = c
			break
		}
	}
	require.NotNil(t, fk, "expected auto-synthesized FK for tenant_id -> tenants")
	assert.Equal(t, []string{"tenant_id"}, fk.Columns)
	assert.Equal(t, "tenants", fk.ReferencedTable)
	assert.Equal(t, []string{"id"}, fk.ReferencedColumns)
	assert.Equal(t, core.RefActionCascade, fk.OnDelete)
	assert.Equal(t, core.RefActionRestrict, fk.OnUpdate)
}

func testUsersInlineUniqueAndCheck(t *testing.T, tbl *core.Table) {
	t.Helper()
	email := tbl.FindColumn("email")
	require.NotNil(t, email)
	assert.True(t, email.Unique)
	assert.Equal(t, "email LIKE '%@%'", email.Check)

	// Auto-synthesized UNIQUE.
	var uqEmail *core.Constraint
	for _, c := range tbl.Constraints {
		if c.Type == core.ConstraintUnique {
			for _, col := range c.Columns {
				if col == "email" {
					uqEmail = c
				}
			}
		}
	}
	require.NotNil(t, uqEmail, "expected auto-synthesized UNIQUE constraint for email")

	// Auto-synthesized CHECK.
	var chkEmail *core.Constraint
	for _, c := range tbl.Constraints {
		if c.Type == core.ConstraintCheck && c.CheckExpression == "email LIKE '%@%'" {
			chkEmail = c
			break
		}
	}
	require.NotNil(t, chkEmail, "expected auto-synthesized CHECK constraint for email")
	assert.True(t, chkEmail.Enforced)
}

func testUsersBooleanDefault(t *testing.T, tbl *core.Table) {
	t.Helper()
	isActive := tbl.FindColumn("is_active")
	require.NotNil(t, isActive)
	assert.Empty(t, isActive.RawType)
	assert.Equal(t, core.DataTypeBoolean, isActive.Type)
	require.NotNil(t, isActive.DefaultValue)
	assert.Equal(t, "TRUE", *isActive.DefaultValue, "native TOML bool should convert to portable TRUE")
}

func testUsersPasswordHash(t *testing.T, tbl *core.Table) {
	t.Helper()
	pwHash := tbl.FindColumn("password_hash")
	require.NotNil(t, pwHash)
	assert.Empty(t, pwHash.RawType)
	assert.Equal(t, core.DataTypeBinary, pwHash.Type)
}

func testUsersNullableDisplayName(t *testing.T, tbl *core.Table) {
	t.Helper()
	displayName := tbl.FindColumn("display_name")
	require.NotNil(t, displayName)
	assert.True(t, displayName.Nullable)
}

func testUsersSimpleIndex(t *testing.T, tbl *core.Table) {
	t.Helper()
	assert.Len(t, tbl.Indexes, 1)
	idx := tbl.FindIndex("idx_users_tenant")
	require.NotNil(t, idx)
	assert.Equal(t, core.IndexTypeBTree, idx.Type)
	assert.Equal(t, core.IndexVisible, idx.Visibility)
	require.Len(t, idx.Columns, 1)
	assert.Equal(t, "tenant_id", idx.Columns[0].Name)
	assert.Equal(t, core.SortAsc, idx.Columns[0].Order)
}

func TestParseFileRoles(t *testing.T) {
	p := NewParser()
	db, err := p.ParseFile(testdataPath("schema.toml"))
	require.NoError(t, err)

	tbl := db.FindTable("roles")
	require.NotNil(t, tbl)

	assert.Equal(t, "RBAC role", tbl.Comment)
	assert.Len(t, tbl.Columns, 5)

	// No timestamps on this table.
	if tbl.Timestamps != nil {
		assert.False(t, tbl.Timestamps.Enabled)
	}

	desc := tbl.FindColumn("description")
	require.NotNil(t, desc)
	assert.True(t, desc.Nullable)

	t.Run("InlineFK", func(t *testing.T) {
		tenantID := tbl.FindColumn("tenant_id")
		require.NotNil(t, tenantID)
		assert.Equal(t, "tenants.id", tenantID.References)

		// Auto-synthesized FK.
		var fk *core.Constraint
		for _, c := range tbl.Constraints {
			if c.Type == core.ConstraintForeignKey && c.ReferencedTable == "tenants" {
				fk = c
				break
			}
		}
		require.NotNil(t, fk, "expected auto-synthesized FK for tenant_id -> tenants")
	})

	t.Run("ExplicitCompositeUnique", func(t *testing.T) {
		uq := tbl.FindConstraint("uq_roles_tenant_name")
		require.NotNil(t, uq)
		assert.Equal(t, core.ConstraintUnique, uq.Type)
		assert.Equal(t, []string{"tenant_id", "name"}, uq.Columns)
	})

	t.Run("AutoPK", func(t *testing.T) {
		pk := tbl.PrimaryKey()
		require.NotNil(t, pk)
		assert.Equal(t, []string{"id"}, pk.Columns)
	})
}

func TestParseFileUserRoles(t *testing.T) {
	p := NewParser()
	db, err := p.ParseFile(testdataPath("schema.toml"))
	require.NoError(t, err)

	tbl := db.FindTable("user_roles")
	require.NotNil(t, tbl)

	assert.Equal(t, "RBAC role assignments (many-to-many)", tbl.Comment)
	assert.Len(t, tbl.Columns, 3)

	// Composite PK via constraints section.
	pk := tbl.PrimaryKey()
	require.NotNil(t, pk)
	assert.Equal(t, []string{"user_id", "role_id"}, pk.Columns)

	t.Run("InlineFKs", func(t *testing.T) {
		userID := tbl.FindColumn("user_id")
		require.NotNil(t, userID)
		assert.Equal(t, "users.id", userID.References)

		roleID := tbl.FindColumn("role_id")
		require.NotNil(t, roleID)
		assert.Equal(t, "roles.id", roleID.References)

		// Auto-synthesized FKs.
		var fkUser, fkRole *core.Constraint
		for _, c := range tbl.Constraints {
			if c.Type == core.ConstraintForeignKey {
				if c.ReferencedTable == "users" {
					fkUser = c
				}
				if c.ReferencedTable == "roles" {
					fkRole = c
				}
			}
		}
		require.NotNil(t, fkUser)
		assert.Equal(t, []string{"user_id"}, fkUser.Columns)
		assert.Equal(t, core.RefActionCascade, fkUser.OnDelete)

		require.NotNil(t, fkRole)
		assert.Equal(t, []string{"role_id"}, fkRole.Columns)
		assert.Equal(t, core.RefActionCascade, fkRole.OnDelete)
	})

	t.Run("SimpleIndex", func(t *testing.T) {
		assert.Len(t, tbl.Indexes, 1)
		idx := tbl.FindIndex("idx_user_roles_role")
		require.NotNil(t, idx)
		require.Len(t, idx.Columns, 1)
		assert.Equal(t, "role_id", idx.Columns[0].Name)
	})
}

func TestParseMinimalSchema(t *testing.T) {
	const schema = `
[database]
name = "testdb"
dialect = "mysql"

[[tables]]
name = "items"

  [[tables.columns]]
  name = "id"
  type = "int"
  primary_key = true
  auto_increment = true

  [[tables.columns]]
  name = "label"
  type = "varchar(100)"
`
	p := NewParser()
	db, err := p.Parse(strings.NewReader(schema))
	require.NoError(t, err)
	require.NotNil(t, db)

	assert.Equal(t, "testdb", db.Name)
	require.Len(t, db.Tables, 1)

	tbl := db.Tables[0]
	assert.Equal(t, "items", tbl.Name)
	assert.Len(t, tbl.Columns, 2)

	// PK auto-synthesized from column-level primary_key = true.
	pk := tbl.PrimaryKey()
	require.NotNil(t, pk)
	assert.Equal(t, []string{"id"}, pk.Columns)

	assert.True(t, tbl.Columns[0].PrimaryKey)
	assert.Equal(t, core.DataTypeInt, tbl.Columns[0].Type)
}
func TestParseValidationRules(t *testing.T) {
	const schema = `
[database]
name = "testdb"
dialect = "mysql"

[validation]
max_table_name_length = 30
max_column_name_length = 30
auto_generate_constraint_names = true
allowed_name_pattern = "^[a-z_]+$"

[[tables]]
name = "items"

  [[tables.columns]]
  name = "id"
  type = "int"
  primary_key = true
`
	p := NewParser()
	db, err := p.Parse(strings.NewReader(schema))
	require.NoError(t, err)
	require.NotNil(t, db.Validation)

	assert.Equal(t, 30, db.Validation.MaxTableNameLength)
	assert.Equal(t, 30, db.Validation.MaxColumnNameLength)
	assert.True(t, db.Validation.AutoGenerateConstraintNames)
	assert.Equal(t, "^[a-z_]+$", db.Validation.AllowedNamePattern)
}

func TestParseValidationRulesRejectsLongTableName(t *testing.T) {
	const schema = `
[database]
name = "testdb"
dialect = "mysql"

[validation]
max_table_name_length = 5

[[tables]]
name = "very_long_table"

  [[tables.columns]]
  name = "id"
  type = "int"
  primary_key = true
`
	p := NewParser()
	_, err := p.Parse(strings.NewReader(schema))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds maximum length")
}

func TestParseValidationRulesRejectsLongColumnName(t *testing.T) {
	const schema = `
[database]
name = "testdb"
dialect = "mysql"

[validation]
max_column_name_length = 3

[[tables]]
name = "t"

  [[tables.columns]]
  name = "very_long_column"
  type = "int"
  primary_key = true
`
	p := NewParser()
	_, err := p.Parse(strings.NewReader(schema))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds maximum length")
}

func TestParseValidationRulesRejectsBadPattern(t *testing.T) {
	const schema = `
[database]
name = "testdb"
dialect = "mysql"

[validation]
allowed_name_pattern = "^[a-z_]+$"

[[tables]]
name = "items123"

  [[tables.columns]]
  name = "id"
  type = "int"
  primary_key = true
`
	p := NewParser()
	_, err := p.Parse(strings.NewReader(schema))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not match allowed pattern")
}

func TestParseUnsupportedDialect(t *testing.T) {
	const schema = `
[database]
name = "testdb"
dialect = "foobardb"

[[tables]]
name = "items"

  [[tables.columns]]
  name = "id"
  type = "int"
  primary_key = true
`
	p := NewParser()
	_, err := p.Parse(strings.NewReader(schema))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported dialect")
	assert.Contains(t, err.Error(), "foobardb")
}

func TestParseInvalidAllowedNamePattern(t *testing.T) {
	const schema = `
[database]
name = "testdb"
dialect = "mysql"

[validation]
allowed_name_pattern = "[invalid(regex"

[[tables]]
name = "items"

  [[tables.columns]]
  name = "id"
  type = "int"
  primary_key = true
`
	p := NewParser()
	_, err := p.Parse(strings.NewReader(schema))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid allowed_name_pattern")
}

func TestParseEmptyTableName(t *testing.T) {
	const schema = `
[database]
name = "testdb"
dialect = "mysql"

[[tables]]
name = ""

  [[tables.columns]]
  name = "id"
  type = "int"
  primary_key = true
`
	p := NewParser()
	_, err := p.Parse(strings.NewReader(schema))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "name is empty")
}

func TestParseWhitespaceOnlyTableName(t *testing.T) {
	const schema = `
[database]
name = "testdb"
dialect = "mysql"

[[tables]]
name = "   "

  [[tables.columns]]
  name = "id"
  type = "int"
  primary_key = true
`
	p := NewParser()
	_, err := p.Parse(strings.NewReader(schema))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "name is empty")
}

func TestParseColumnNamePatternMismatch(t *testing.T) {
	const schema = `
[database]
name = "testdb"
dialect = "mysql"

[validation]
allowed_name_pattern = "^[a-z_]+$"

[[tables]]
name = "items"

  [[tables.columns]]
  name = "col123"
  type = "int"
  primary_key = true
`
	p := NewParser()
	_, err := p.Parse(strings.NewReader(schema))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not match allowed pattern")
	assert.Contains(t, err.Error(), "col123")
}

func TestParseInvalidRawTypeForDialect(t *testing.T) {
	const schema = `
[database]
name = "testdb"
dialect = "mysql"

[[tables]]
name = "items"

  [[tables.columns]]
  name     = "data"
  type     = "text"
  raw_type = "TOTALLY_FAKE_TYPE"

  [[tables.constraints]]
  type    = "PRIMARY KEY"
  columns = ["data"]
`
	p := NewParser()
	_, err := p.Parse(strings.NewReader(schema))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "TOTALLY_FAKE_TYPE")
}

func TestParseMySQLColumnOptions(t *testing.T) {
	const schema = `
[database]
name = "testdb"
dialect = "mysql"

[[tables]]
name = "items"

  [[tables.columns]]
  name = "id"
  type = "int"
  primary_key = true

    [tables.columns.mysql]
    column_format              = "FIXED"
    storage                    = "DISK"
    secondary_engine_attribute = '{"key":"val"}'
`
	p := NewParser()
	db, err := p.Parse(strings.NewReader(schema))
	require.NoError(t, err)

	col := db.Tables[0].FindColumn("id")
	require.NotNil(t, col)
	require.NotNil(t, col.MySQL)
	assert.Equal(t, "FIXED", col.MySQL.ColumnFormat)
	assert.Equal(t, "DISK", col.MySQL.Storage)
	assert.JSONEq(t, `{"key":"val"}`, col.MySQL.SecondaryEngineAttribute)
}

func TestParseTiDBColumnOptions(t *testing.T) {
	const schema = `
[database]
name = "testdb"
dialect = "mysql"

[[tables]]
name = "items"

  [[tables.columns]]
  name = "id"
  type = "int"
  primary_key = true

    [tables.columns.tidb]
    shard_bits = 5
`
	p := NewParser()
	db, err := p.Parse(strings.NewReader(schema))
	require.NoError(t, err)

	col := db.Tables[0].FindColumn("id")
	require.NotNil(t, col)
	require.NotNil(t, col.TiDB)
	assert.Equal(t, uint64(5), col.TiDB.ShardBits)
}

func TestParseFloatDefaultValue(t *testing.T) {
	const schema = `
[database]
name = "testdb"
dialect = "mysql"

[[tables]]
name = "items"

  [[tables.columns]]
  name        = "score"
  type        = "float"
  primary_key = true
  default     = 3.14
`
	p := NewParser()
	db, err := p.Parse(strings.NewReader(schema))
	require.NoError(t, err)

	col := db.Tables[0].FindColumn("score")
	require.NotNil(t, col)
	require.NotNil(t, col.DefaultValue)
	assert.Equal(t, "3.14", *col.DefaultValue)
}

func TestParseDatetimeDefaultValue(t *testing.T) {
	const schema = `
[database]
name = "testdb"
dialect = "mysql"

[[tables]]
name = "items"

  [[tables.columns]]
  name        = "created"
  type        = "timestamp"
  primary_key = true
  default     = 2024-01-01T00:00:00Z
`
	p := NewParser()
	db, err := p.Parse(strings.NewReader(schema))
	require.NoError(t, err)

	col := db.Tables[0].FindColumn("created")
	require.NotNil(t, col)
	require.NotNil(t, col.DefaultValue)
	// time.Time hits the default case in normalizeDefault using fmt.Sprintf
	assert.NotEmpty(t, *col.DefaultValue)
	assert.Contains(t, *col.DefaultValue, "2024")
}

func TestParseInvalidToml(t *testing.T) {
	p := NewParser()
	_, err := p.Parse(strings.NewReader(`this is not valid toml {{{`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "decode error")
}

func TestParseFileFileNotFound(t *testing.T) {
	p := NewParser()
	_, err := p.ParseFile("/nonexistent/path/schema.toml")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "open file")
}

func TestParseFileExampleSchemaToml(t *testing.T) {
	p := NewParser()
	db, err := p.ParseFile(testdataPath("example_schema.toml"))
	require.NoError(t, err)
	require.NotNil(t, db)

	assert.Equal(t, "ecommerce", db.Name)
	require.NotNil(t, db.Dialect)
	assert.Equal(t, core.DialectMySQL, *db.Dialect)
	assert.Len(t, db.Tables, 4)

	// Verify all table names.
	want := []string{"tenants", "users", "roles", "user_roles"}
	for i, name := range want {
		assert.Equal(t, name, db.Tables[i].Name)
	}
}
