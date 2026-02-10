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

// ---------------------------------------------------------------------------
// Full schema.toml integration tests
// ---------------------------------------------------------------------------

func TestParseFileSchemaToml(t *testing.T) {
	p := NewParser()
	db, err := p.ParseFile(testdataPath("schema.toml"))
	require.NoError(t, err)
	require.NotNil(t, db)

	assert.Equal(t, "ecommerce", db.Name)
	assert.Equal(t, "mysql", db.Dialect)
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
	assert.Equal(t, "InnoDB", tbl.Options.Engine)
	assert.Equal(t, "utf8mb4", tbl.Options.Charset)
	assert.Equal(t, "utf8mb4_unicode_ci", tbl.Options.Collate)

	// Timestamps enabled — created_at and updated_at injected.
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
	assert.Equal(t, "bigint", id.TypeRaw)
	assert.Equal(t, core.DataTypeInt, id.Type)
	assert.True(t, id.PrimaryKey)
	assert.True(t, id.AutoIncrement)
	assert.False(t, id.Nullable)

	slug := tbl.FindColumn("slug")
	require.NotNil(t, slug)
	assert.Equal(t, "varchar(64)", slug.TypeRaw)
	assert.Equal(t, core.DataTypeString, slug.Type)
	assert.True(t, slug.Unique, "slug should have inline unique = true")

	name := tbl.FindColumn("name")
	require.NotNil(t, name)
	assert.Equal(t, "varchar(255)", name.TypeRaw)

	plan := tbl.FindColumn("plan")
	require.NotNil(t, plan)
	// v2: type = "enum" + values = [...] -> TypeRaw built from values.
	assert.Equal(t, "enum('free','pro','enterprise')", plan.TypeRaw)
	assert.Equal(t, core.DataTypeEnum, plan.Type)
	assert.Equal(t, []string{"free", "pro", "enterprise"}, plan.EnumValues)
	require.NotNil(t, plan.DefaultValue)
	assert.Equal(t, "free", *plan.DefaultValue)

	settings := tbl.FindColumn("settings")
	require.NotNil(t, settings)
	assert.Equal(t, "json", settings.TypeRaw)
	assert.Equal(t, core.DataTypeJSON, settings.Type)
	assert.True(t, settings.Nullable)

	// Timestamps injected columns.
	createdAt := tbl.FindColumn("created_at")
	require.NotNil(t, createdAt)
	assert.Equal(t, "timestamp", createdAt.TypeRaw)
	assert.Equal(t, core.DataTypeDatetime, createdAt.Type)
	require.NotNil(t, createdAt.DefaultValue)
	assert.Equal(t, "CURRENT_TIMESTAMP", *createdAt.DefaultValue)

	updatedAt := tbl.FindColumn("updated_at")
	require.NotNil(t, updatedAt)
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
	assert.Equal(t, "boolean", isActive.TypeRaw)
	assert.Equal(t, core.DataTypeBoolean, isActive.Type)
	require.NotNil(t, isActive.DefaultValue)
	assert.Equal(t, "TRUE", *isActive.DefaultValue, "native TOML bool should convert to portable TRUE")
}

func testUsersPasswordHash(t *testing.T, tbl *core.Table) {
	t.Helper()
	pwHash := tbl.FindColumn("password_hash")
	require.NotNil(t, pwHash)
	assert.Equal(t, "varbinary(60)", pwHash.TypeRaw)
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

// ---------------------------------------------------------------------------
// Unit-level TOML snippet tests
// ---------------------------------------------------------------------------

func TestParseMinimalSchema(t *testing.T) {
	const schema = `
[database]
name = "testdb"

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

func TestParseSchemaVersion(t *testing.T) {
	const schema = `
[database]
name = "testdb"
version = "2.3.1"

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
	assert.Equal(t, "2.3.1", db.Version)
}

func TestParseValidationRules(t *testing.T) {
	const schema = `
[database]
name = "testdb"

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

func TestParseRawTypeDialectScoped(t *testing.T) {
	const schema = `
[database]
name = "testdb"
dialect = "postgresql"

[[tables]]
name = "items"

  [[tables.columns]]
  name     = "data"
  type     = "json"
  raw_type = "JSONB"

  [[tables.constraints]]
  type    = "PRIMARY KEY"
  columns = ["data"]
`
	p := NewParser()
	db, err := p.Parse(strings.NewReader(schema))
	require.NoError(t, err)

	col := db.Tables[0].FindColumn("data")
	require.NotNil(t, col)
	assert.Equal(t, "json", col.TypeRaw, "TypeRaw should be the portable type")
	require.NotEmpty(t, col.RawType)
	assert.Equal(t, "JSONB", col.RawType)
	assert.Equal(t, "postgresql", col.RawTypeDialect)

	// EffectiveType should return override for matching dialect.
	assert.Equal(t, "JSONB", col.EffectiveType("postgresql"))
	assert.Equal(t, "json", col.EffectiveType("mysql"))
}

func TestParseRawTypeNoDialect(t *testing.T) {
	const schema = `
[database]
name = "testdb"

[[tables]]
name = "items"

  [[tables.columns]]
  name     = "data"
  type     = "json"
  raw_type = "JSONB"

  [[tables.constraints]]
  type    = "PRIMARY KEY"
  columns = ["data"]
`
	p := NewParser()
	db, err := p.Parse(strings.NewReader(schema))
	require.NoError(t, err)

	col := db.Tables[0].FindColumn("data")
	require.NotNil(t, col)
	assert.Equal(t, "json", col.TypeRaw)
	// raw_type should be ignored when dialect is empty.
	assert.Empty(t, col.RawType)
	assert.Empty(t, col.RawTypeDialect)
}

func TestParseNullableDefaultFalse(t *testing.T) {
	const schema = `
[database]
name = "testdb"

[[tables]]
name = "items"

  [[tables.columns]]
  name = "id"
  type = "int"
  primary_key = true

  [[tables.columns]]
  name     = "note"
  type     = "text"
  nullable = true
`
	p := NewParser()
	db, err := p.Parse(strings.NewReader(schema))
	require.NoError(t, err)

	id := db.Tables[0].FindColumn("id")
	require.NotNil(t, id)
	assert.False(t, id.Nullable)

	note := db.Tables[0].FindColumn("note")
	require.NotNil(t, note)
	assert.True(t, note.Nullable)
}

func TestParseOptionalFieldsNilWhenAbsent(t *testing.T) {
	const schema = `
[database]
name = "testdb"

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

	col := db.Tables[0].Columns[0]
	assert.Nil(t, col.DefaultValue)
	assert.Nil(t, col.OnUpdate)
	assert.Empty(t, col.References)
	assert.Empty(t, col.Check)
	assert.False(t, col.Unique)
	assert.Nil(t, col.EnumValues)
	assert.Empty(t, col.RawType)
	assert.Empty(t, col.RawTypeDialect)
}

func TestParseBooleanDefaultValue(t *testing.T) {
	const schema = `
[database]
name = "testdb"

[[tables]]
name = "items"

  [[tables.columns]]
  name        = "active"
  type        = "boolean"
  primary_key = true
  default     = true

  [[tables.columns]]
  name    = "deleted"
  type    = "boolean"
  default = false
`
	p := NewParser()
	db, err := p.Parse(strings.NewReader(schema))
	require.NoError(t, err)

	active := db.Tables[0].FindColumn("active")
	require.NotNil(t, active)
	require.NotNil(t, active.DefaultValue)
	assert.Equal(t, "TRUE", *active.DefaultValue)

	deleted := db.Tables[0].FindColumn("deleted")
	require.NotNil(t, deleted)
	require.NotNil(t, deleted.DefaultValue)
	assert.Equal(t, "FALSE", *deleted.DefaultValue)
}

func TestParseIntegerDefaultValue(t *testing.T) {
	const schema = `
[database]
name = "testdb"

[[tables]]
name = "items"

  [[tables.columns]]
  name        = "priority"
  type        = "int"
  primary_key = true
  default     = 42
`
	p := NewParser()
	db, err := p.Parse(strings.NewReader(schema))
	require.NoError(t, err)

	col := db.Tables[0].FindColumn("priority")
	require.NotNil(t, col)
	require.NotNil(t, col.DefaultValue)
	assert.Equal(t, "42", *col.DefaultValue)
}

func TestParseStringDefaultValue(t *testing.T) {
	const schema = `
[database]
name = "testdb"

[[tables]]
name = "items"

  [[tables.columns]]
  name        = "status"
  type        = "varchar(20)"
  primary_key = true
  default     = "pending"
`
	p := NewParser()
	db, err := p.Parse(strings.NewReader(schema))
	require.NoError(t, err)

	col := db.Tables[0].FindColumn("status")
	require.NotNil(t, col)
	require.NotNil(t, col.DefaultValue)
	assert.Equal(t, "pending", *col.DefaultValue)
}

func TestParseDefaultValueAndOnUpdateNoFK(t *testing.T) {
	const schema = `
[database]
name = "testdb"

[[tables]]
name = "items"

  [[tables.columns]]
  name      = "updated_at"
  type      = "timestamp"
  default   = "CURRENT_TIMESTAMP"
  on_update = "CURRENT_TIMESTAMP"

  [[tables.constraints]]
  type    = "PRIMARY KEY"
  columns = ["updated_at"]
`
	p := NewParser()
	db, err := p.Parse(strings.NewReader(schema))
	require.NoError(t, err)

	col := db.Tables[0].Columns[0]
	require.NotNil(t, col.DefaultValue)
	assert.Equal(t, "CURRENT_TIMESTAMP", *col.DefaultValue)
	require.NotNil(t, col.OnUpdate)
	assert.Equal(t, "CURRENT_TIMESTAMP", *col.OnUpdate)
	// No FK -> on_update is NOT a referential action.
	assert.Empty(t, col.References)
	assert.Equal(t, core.RefActionNone, col.RefOnUpdate)
}

func TestParseEnumWithValuesArray(t *testing.T) {
	const schema = `
[database]
name = "testdb"

[[tables]]
name = "items"

  [[tables.columns]]
  name        = "status"
  type        = "enum"
  values      = ["active", "paused", "deleted"]
  default     = "active"
  primary_key = true
`
	p := NewParser()
	db, err := p.Parse(strings.NewReader(schema))
	require.NoError(t, err)

	col := db.Tables[0].FindColumn("status")
	require.NotNil(t, col)
	assert.Equal(t, core.DataTypeEnum, col.Type)
	assert.Equal(t, "enum('active','paused','deleted')", col.TypeRaw)
	assert.Equal(t, []string{"active", "paused", "deleted"}, col.EnumValues)
	require.NotNil(t, col.DefaultValue)
	assert.Equal(t, "active", *col.DefaultValue)
}

func TestParseEnumWithQuotesInValues(t *testing.T) {
	const schema = `
[database]
name = "testdb"

[[tables]]
name = "items"

  [[tables.columns]]
  name        = "label"
  type        = "enum"
  values      = ["it's", "they're"]
  primary_key = true
`
	p := NewParser()
	db, err := p.Parse(strings.NewReader(schema))
	require.NoError(t, err)

	col := db.Tables[0].FindColumn("label")
	require.NotNil(t, col)
	assert.Equal(t, "enum('it''s','they''re')", col.TypeRaw, "single quotes in values should be escaped")
}

func TestParseInlineFK(t *testing.T) {
	const schema = `
[database]
name = "testdb"

[[tables]]
name = "parents"

  [[tables.columns]]
  name = "id"
  type = "int"
  primary_key = true

[[tables]]
name = "children"

  [[tables.columns]]
  name = "id"
  type = "int"
  primary_key = true

  [[tables.columns]]
  name       = "parent_id"
  type       = "int"
  references = "parents.id"
  on_delete  = "CASCADE"
  on_update  = "SET NULL"
`
	p := NewParser()
	db, err := p.Parse(strings.NewReader(schema))
	require.NoError(t, err)

	tbl := db.FindTable("children")
	require.NotNil(t, tbl)

	col := tbl.FindColumn("parent_id")
	require.NotNil(t, col)
	assert.Equal(t, "parents.id", col.References)
	assert.Equal(t, core.RefActionCascade, col.RefOnDelete)
	assert.Equal(t, core.ReferentialAction("SET NULL"), col.RefOnUpdate)

	// on_update is routed to RefOnUpdate, NOT to OnUpdate (timestamp).
	assert.Nil(t, col.OnUpdate)

	// Auto-synthesized FK constraint.
	var fk *core.Constraint
	for _, c := range tbl.Constraints {
		if c.Type == core.ConstraintForeignKey {
			fk = c
			break
		}
	}
	require.NotNil(t, fk)
	assert.Equal(t, "fk_children_parents", fk.Name)
	assert.Equal(t, []string{"parent_id"}, fk.Columns)
	assert.Equal(t, "parents", fk.ReferencedTable)
	assert.Equal(t, []string{"id"}, fk.ReferencedColumns)
	assert.Equal(t, core.RefActionCascade, fk.OnDelete)
	assert.Equal(t, core.ReferentialAction("SET NULL"), fk.OnUpdate)
}

func TestParseInlineUnique(t *testing.T) {
	const schema = `
[database]
name = "testdb"

[[tables]]
name = "items"

  [[tables.columns]]
  name        = "id"
  type        = "int"
  primary_key = true

  [[tables.columns]]
  name   = "code"
  type   = "varchar(50)"
  unique = true
`
	p := NewParser()
	db, err := p.Parse(strings.NewReader(schema))
	require.NoError(t, err)

	tbl := db.Tables[0]

	// Auto-synthesized UNIQUE constraint.
	var uq *core.Constraint
	for _, c := range tbl.Constraints {
		if c.Type == core.ConstraintUnique {
			uq = c
			break
		}
	}
	require.NotNil(t, uq)
	assert.Equal(t, "uq_items_code", uq.Name)
	assert.Equal(t, []string{"code"}, uq.Columns)
}

func TestParseInlineCheck(t *testing.T) {
	const schema = `
[database]
name = "testdb"

[[tables]]
name = "items"

  [[tables.columns]]
  name        = "age"
  type        = "int"
  primary_key = true
  check       = "age >= 0 AND age <= 200"
`
	p := NewParser()
	db, err := p.Parse(strings.NewReader(schema))
	require.NoError(t, err)

	tbl := db.Tables[0]

	// Auto-synthesized CHECK constraint.
	var chk *core.Constraint
	for _, c := range tbl.Constraints {
		if c.Type == core.ConstraintCheck {
			chk = c
			break
		}
	}
	require.NotNil(t, chk)
	assert.Equal(t, "chk_items_age", chk.Name)
	assert.Equal(t, "age >= 0 AND age <= 200", chk.CheckExpression)
	assert.True(t, chk.Enforced)
}

func TestParsePKAutoSynthesisedFromColumn(t *testing.T) {
	const schema = `
[database]
name = "testdb"

[[tables]]
name = "items"

  [[tables.columns]]
  name        = "id"
  type        = "int"
  primary_key = true
`
	p := NewParser()
	db, err := p.Parse(strings.NewReader(schema))
	require.NoError(t, err)

	pk := db.Tables[0].PrimaryKey()
	require.NotNil(t, pk, "PK should be auto-synthesized from column-level primary_key = true")
	assert.Equal(t, "pk_items", pk.Name)
	assert.Equal(t, []string{"id"}, pk.Columns)
}

func TestParsePKExplicitConstraint(t *testing.T) {
	const schema = `
[database]
name = "testdb"

[[tables]]
name = "items"

  [[tables.columns]]
  name = "a"
  type = "int"

  [[tables.columns]]
  name = "b"
  type = "int"

  [[tables.constraints]]
  type    = "PRIMARY KEY"
  columns = ["a", "b"]
`
	p := NewParser()
	db, err := p.Parse(strings.NewReader(schema))
	require.NoError(t, err)

	pk := db.Tables[0].PrimaryKey()
	require.NotNil(t, pk)
	assert.Equal(t, []string{"a", "b"}, pk.Columns)

	// No column has primary_key = true, so no conflict.
	for _, col := range db.Tables[0].Columns {
		assert.False(t, col.PrimaryKey)
	}
}

func TestParsePKConflictErrors(t *testing.T) {
	const schema = `
[database]
name = "testdb"

[[tables]]
name = "items"

  [[tables.columns]]
  name        = "id"
  type        = "int"
  primary_key = true

  [[tables.constraints]]
  type    = "PRIMARY KEY"
  columns = ["id"]
`
	p := NewParser()
	_, err := p.Parse(strings.NewReader(schema))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "primary key declared on both")
}

func TestParseConstraintEnforcedDefault(t *testing.T) {
	const schema = `
[database]
name = "testdb"

[[tables]]
name = "items"

  [[tables.columns]]
  name = "val"
  type = "int"

  [[tables.constraints]]
  type    = "PRIMARY KEY"
  columns = ["val"]

  [[tables.constraints]]
  type             = "CHECK"
  check_expression = "val > 0"

  [[tables.constraints]]
  type             = "CHECK"
  check_expression = "val < 1000"
  enforced         = false
`
	p := NewParser()
	db, err := p.Parse(strings.NewReader(schema))
	require.NoError(t, err)

	// Index 0 = PK, 1 = first CHECK, 2 = second CHECK.
	require.True(t, len(db.Tables[0].Constraints) >= 3)
	assert.True(t, db.Tables[0].Constraints[1].Enforced)
	assert.False(t, db.Tables[0].Constraints[2].Enforced)
}

func TestParseExplicitForeignKeyConstraint(t *testing.T) {
	const schema = `
[database]
name = "testdb"

[[tables]]
name = "parents"

  [[tables.columns]]
  name = "id"
  type = "int"
  primary_key = true

[[tables]]
name = "children"

  [[tables.columns]]
  name = "id"
  type = "int"
  primary_key = true

  [[tables.columns]]
  name = "parent_id"
  type = "int"

  [[tables.constraints]]
  name               = "fk_child_parent"
  type               = "FOREIGN KEY"
  columns            = ["parent_id"]
  referenced_table   = "parents"
  referenced_columns = ["id"]
  on_delete          = "CASCADE"
  on_update          = "SET NULL"
`
	p := NewParser()
	db, err := p.Parse(strings.NewReader(schema))
	require.NoError(t, err)

	tbl := db.FindTable("children")
	require.NotNil(t, tbl)

	fk := tbl.FindConstraint("fk_child_parent")
	require.NotNil(t, fk)

	assert.Equal(t, core.ConstraintForeignKey, fk.Type)
	assert.Equal(t, []string{"parent_id"}, fk.Columns)
	assert.Equal(t, "parents", fk.ReferencedTable)
	assert.Equal(t, []string{"id"}, fk.ReferencedColumns)
	assert.Equal(t, core.RefActionCascade, fk.OnDelete)
	assert.Equal(t, core.ReferentialAction("SET NULL"), fk.OnUpdate)
}

// ---------------------------------------------------------------------------
// Simple index columns (string array)
// ---------------------------------------------------------------------------

func TestParseIndexSimpleColumns(t *testing.T) {
	const schema = `
[database]
name = "testdb"

[[tables]]
name = "items"

  [[tables.columns]]
  name = "id"
  type = "int"
  primary_key = true

  [[tables.columns]]
  name = "a"
  type = "int"

  [[tables.columns]]
  name = "b"
  type = "int"

  [[tables.indexes]]
  name    = "idx_composite"
  columns = ["a", "b"]
`
	p := NewParser()
	db, err := p.Parse(strings.NewReader(schema))
	require.NoError(t, err)

	idx := db.Tables[0].FindIndex("idx_composite")
	require.NotNil(t, idx)

	require.Len(t, idx.Columns, 2)
	assert.Equal(t, "a", idx.Columns[0].Name)
	assert.Equal(t, core.SortAsc, idx.Columns[0].Order)
	assert.Equal(t, "b", idx.Columns[1].Name)
	assert.Equal(t, core.SortAsc, idx.Columns[1].Order)
}

func TestParseIndexAdvancedColumnDefs(t *testing.T) {
	const schema = `
[database]
name = "testdb"

[[tables]]
name = "items"

  [[tables.columns]]
  name = "id"
  type = "int"
  primary_key = true

  [[tables.columns]]
  name = "label"
  type = "varchar(100)"

  [[tables.indexes]]
  name       = "idx_items_label"
  unique     = true
  type       = "HASH"
  visibility = "INVISIBLE"
  comment    = "fast label lookup"

    [[tables.indexes.column_defs]]
    name   = "label"
    length = 20
    order  = "DESC"
`
	p := NewParser()
	db, err := p.Parse(strings.NewReader(schema))
	require.NoError(t, err)

	idx := db.Tables[0].FindIndex("idx_items_label")
	require.NotNil(t, idx)

	assert.True(t, idx.Unique)
	assert.Equal(t, core.IndexTypeHash, idx.Type)
	assert.Equal(t, core.IndexInvisible, idx.Visibility)
	assert.Equal(t, "fast label lookup", idx.Comment)

	require.Len(t, idx.Columns, 1)
	assert.Equal(t, "label", idx.Columns[0].Name)
	assert.Equal(t, 20, idx.Columns[0].Length)
	assert.Equal(t, core.SortDesc, idx.Columns[0].Order)
}

func TestParseIndexDefaultValues(t *testing.T) {
	const schema = `
[database]
name = "testdb"

[[tables]]
name = "items"

  [[tables.columns]]
  name = "id"
  type = "int"
  primary_key = true

  [[tables.indexes]]
  name    = "idx_items_id"
  columns = ["id"]
`
	p := NewParser()
	db, err := p.Parse(strings.NewReader(schema))
	require.NoError(t, err)

	require.Len(t, db.Tables[0].Indexes, 1)
	idx := db.Tables[0].Indexes[0]

	assert.Equal(t, core.IndexTypeBTree, idx.Type)
	assert.Equal(t, core.IndexVisible, idx.Visibility)
	assert.False(t, idx.Unique)

	require.Len(t, idx.Columns, 1)
	assert.Equal(t, core.SortAsc, idx.Columns[0].Order)
}

// ---------------------------------------------------------------------------
// Timestamps injection
// ---------------------------------------------------------------------------

func TestParseTimestampsInjection(t *testing.T) {
	const schema = `
[database]
name = "testdb"

[[tables]]
name = "items"

  [tables.timestamps]
  enabled = true

  [[tables.columns]]
  name = "id"
  type = "int"
  primary_key = true
`
	p := NewParser()
	db, err := p.Parse(strings.NewReader(schema))
	require.NoError(t, err)

	tbl := db.Tables[0]
	require.NotNil(t, tbl.Timestamps)
	assert.True(t, tbl.Timestamps.Enabled)

	// 1 declared + 2 injected = 3.
	assert.Len(t, tbl.Columns, 3)

	createdAt := tbl.FindColumn("created_at")
	require.NotNil(t, createdAt)
	assert.Equal(t, "timestamp", createdAt.TypeRaw)
	require.NotNil(t, createdAt.DefaultValue)
	assert.Equal(t, "CURRENT_TIMESTAMP", *createdAt.DefaultValue)

	updatedAt := tbl.FindColumn("updated_at")
	require.NotNil(t, updatedAt)
	assert.Equal(t, "timestamp", updatedAt.TypeRaw)
	require.NotNil(t, updatedAt.DefaultValue)
	assert.Equal(t, "CURRENT_TIMESTAMP", *updatedAt.DefaultValue)
	require.NotNil(t, updatedAt.OnUpdate)
	assert.Equal(t, "CURRENT_TIMESTAMP", *updatedAt.OnUpdate)
}

func TestParseTimestampsCustomColumnNames(t *testing.T) {
	const schema = `
[database]
name = "testdb"

[[tables]]
name = "items"

  [tables.timestamps]
  enabled        = true
  created_column = "inserted_at"
  updated_column = "modified_at"

  [[tables.columns]]
  name = "id"
  type = "int"
  primary_key = true
`
	p := NewParser()
	db, err := p.Parse(strings.NewReader(schema))
	require.NoError(t, err)

	tbl := db.Tables[0]
	assert.Len(t, tbl.Columns, 3)

	assert.NotNil(t, tbl.FindColumn("inserted_at"))
	assert.NotNil(t, tbl.FindColumn("modified_at"))
	assert.Nil(t, tbl.FindColumn("created_at"))
	assert.Nil(t, tbl.FindColumn("updated_at"))
}

func TestParseTimestampsSkipIfColumnsExist(t *testing.T) {
	const schema = `
[database]
name = "testdb"

[[tables]]
name = "items"

  [tables.timestamps]
  enabled = true

  [[tables.columns]]
  name = "id"
  type = "int"
  primary_key = true

  [[tables.columns]]
  name    = "created_at"
  type    = "timestamp"
  default = "CUSTOM_VALUE"
`
	p := NewParser()
	db, err := p.Parse(strings.NewReader(schema))
	require.NoError(t, err)

	tbl := db.Tables[0]
	// created_at already exists -> not injected again.
	// updated_at doesn't exist -> injected.
	assert.Len(t, tbl.Columns, 3)

	createdAt := tbl.FindColumn("created_at")
	require.NotNil(t, createdAt)
	require.NotNil(t, createdAt.DefaultValue)
	assert.Equal(t, "CUSTOM_VALUE", *createdAt.DefaultValue, "existing column should not be overwritten")
}

func TestParseTimestampsDisabled(t *testing.T) {
	const schema = `
[database]
name = "testdb"

[[tables]]
name = "items"

  [tables.timestamps]
  enabled = false

  [[tables.columns]]
  name = "id"
  type = "int"
  primary_key = true
`
	p := NewParser()
	db, err := p.Parse(strings.NewReader(schema))
	require.NoError(t, err)

	tbl := db.Tables[0]
	assert.Len(t, tbl.Columns, 1, "timestamps disabled -> no injection")
}

// ---------------------------------------------------------------------------
// Generated columns
// ---------------------------------------------------------------------------

func TestParseGeneratedColumn(t *testing.T) {
	const schema = `
[database]
name = "testdb"

[[tables]]
name = "items"

  [[tables.columns]]
  name                  = "id"
  type                  = "int"
  primary_key           = true

  [[tables.columns]]
  name                  = "full_name"
  type                  = "varchar(255)"
  is_generated          = true
  generation_expression = "CONCAT(first_name, ' ', last_name)"
  generation_storage    = "STORED"
`
	p := NewParser()
	db, err := p.Parse(strings.NewReader(schema))
	require.NoError(t, err)

	col := db.Tables[0].FindColumn("full_name")
	require.NotNil(t, col)
	assert.True(t, col.IsGenerated)
	assert.Equal(t, "CONCAT(first_name, ' ', last_name)", col.GenerationExpression)
	assert.Equal(t, core.GenerationStored, col.GenerationStorage)
}

// ---------------------------------------------------------------------------
// Table options
// ---------------------------------------------------------------------------

func TestParseTableOptions(t *testing.T) {
	const schema = `
[database]
name = "testdb"

[[tables]]
name = "items"

  [tables.options]
  engine         = "InnoDB"
  charset        = "utf8mb4"
  collate        = "utf8mb4_general_ci"
  row_format     = "COMPRESSED"
  tablespace     = "ts1"
  compression    = "zlib"
  encryption     = "Y"
  key_block_size = 8

  [[tables.columns]]
  name = "id"
  type = "int"
  primary_key = true
`
	p := NewParser()
	db, err := p.Parse(strings.NewReader(schema))
	require.NoError(t, err)

	opts := db.Tables[0].Options
	assert.Equal(t, "InnoDB", opts.Engine)
	assert.Equal(t, "utf8mb4", opts.Charset)
	assert.Equal(t, "utf8mb4_general_ci", opts.Collate)
	assert.Equal(t, "COMPRESSED", opts.RowFormat)
	assert.Equal(t, "ts1", opts.Tablespace)
	assert.Equal(t, "zlib", opts.Compression)
	assert.Equal(t, "Y", opts.Encryption)
	assert.Equal(t, uint64(8), opts.KeyBlockSize)
}

// ---------------------------------------------------------------------------
// Data-type normalisation through the parser
// ---------------------------------------------------------------------------

func TestParseDataTypeNormalization(t *testing.T) {
	tests := []struct {
		rawType  string
		expected core.DataType
	}{
		{"varchar(255)", core.DataTypeString},
		{"char(10)", core.DataTypeString},
		{"text", core.DataTypeString},
		{"int", core.DataTypeInt},
		{"bigint", core.DataTypeInt},
		{"smallint", core.DataTypeInt},
		{"boolean", core.DataTypeBoolean},
		{"float", core.DataTypeFloat},
		{"double", core.DataTypeFloat},
		{"decimal(10,2)", core.DataTypeFloat},
		{"timestamp", core.DataTypeDatetime},
		{"datetime", core.DataTypeDatetime},
		{"date", core.DataTypeDatetime},
		{"json", core.DataTypeJSON},
		{"uuid", core.DataTypeUUID},
		{"blob", core.DataTypeBinary},
		{"varbinary(60)", core.DataTypeBinary},
		{"binary(16)", core.DataTypeBinary},
	}

	for _, tt := range tests {
		t.Run(tt.rawType, func(t *testing.T) {
			schema := `
[database]
name = "testdb"

[[tables]]
name = "items"

  [[tables.columns]]
  name = "col"
  type = "` + tt.rawType + `"
  primary_key = true
`
			p := NewParser()
			db, err := p.Parse(strings.NewReader(schema))
			require.NoError(t, err)

			col := db.Tables[0].Columns[0]
			assert.Equal(t, tt.expected, col.Type)
		})
	}
}

// ---------------------------------------------------------------------------
// Error cases
// ---------------------------------------------------------------------------

func TestParseInvalidToml(t *testing.T) {
	p := NewParser()
	_, err := p.Parse(strings.NewReader(`this is not valid toml {{{`))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "decode error")
}

func TestParseFileFileNotFound(t *testing.T) {
	p := NewParser()
	_, err := p.ParseFile("/nonexistent/path/schema.toml")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "open file")
}

func TestParseEmptyTable(t *testing.T) {
	const schema = `
[database]
name = "testdb"

[[tables]]
name = "empty"
`
	p := NewParser()
	_, err := p.Parse(strings.NewReader(schema))
	assert.Error(t, err)
}

func TestParseEmptyColumnName(t *testing.T) {
	const schema = `
[database]
name = "testdb"

[[tables]]
name = "items"

  [[tables.columns]]
  name = ""
  type = "int"

  [[tables.constraints]]
  type    = "PRIMARY KEY"
  columns = [""]
`
	p := NewParser()
	_, err := p.Parse(strings.NewReader(schema))
	assert.Error(t, err)
}

func TestParseEmptyColumnType(t *testing.T) {
	const schema = `
[database]
name = "testdb"

[[tables]]
name = "items"

  [[tables.columns]]
  name = "id"

  [[tables.constraints]]
  type    = "PRIMARY KEY"
  columns = ["id"]
`
	p := NewParser()
	_, err := p.Parse(strings.NewReader(schema))
	assert.Error(t, err)
}

func TestParseDuplicateTableName(t *testing.T) {
	const schema = `
[database]
name = "testdb"

[[tables]]
name = "items"

  [[tables.columns]]
  name = "id"
  type = "int"
  primary_key = true

[[tables]]
name = "items"

  [[tables.columns]]
  name = "id"
  type = "int"
  primary_key = true
`
	p := NewParser()
	_, err := p.Parse(strings.NewReader(schema))
	assert.Error(t, err)
}

func TestParseValidationRulesRejectsLongTableName(t *testing.T) {
	const schema = `
[database]
name = "testdb"

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

[validation]
allowed_name_pattern = "^[a-z_]+$"

[[tables]]
name = "Items"

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

// ---------------------------------------------------------------------------
// Edge: multiple inline shortcuts on one column
// ---------------------------------------------------------------------------

func TestParseColumnWithMultipleShortcuts(t *testing.T) {
	const schema = `
[database]
name = "testdb"

[[tables]]
name = "items"

  [[tables.columns]]
  name   = "id"
  type   = "int"
  primary_key = true

  [[tables.columns]]
  name       = "ref_id"
  type       = "int"
  unique     = true
  check      = "ref_id > 0"
  references = "other.id"
  on_delete  = "CASCADE"
`
	p := NewParser()
	db, err := p.Parse(strings.NewReader(schema))
	require.NoError(t, err)

	tbl := db.Tables[0]
	col := tbl.FindColumn("ref_id")
	require.NotNil(t, col)
	assert.True(t, col.Unique)
	assert.Equal(t, "ref_id > 0", col.Check)
	assert.Equal(t, "other.id", col.References)
	assert.Equal(t, core.RefActionCascade, col.RefOnDelete)

	// Should produce PK + UNIQUE + CHECK + FK = 4 constraints.
	pkCount, uqCount, chkCount, fkCount := 0, 0, 0, 0
	for _, c := range tbl.Constraints {
		switch c.Type {
		case core.ConstraintPrimaryKey:
			pkCount++
		case core.ConstraintUnique:
			uqCount++
		case core.ConstraintCheck:
			chkCount++
		case core.ConstraintForeignKey:
			fkCount++
		}
	}
	assert.Equal(t, 1, pkCount)
	assert.Equal(t, 1, uqCount)
	assert.Equal(t, 1, chkCount)
	assert.Equal(t, 1, fkCount)
}

// ---------------------------------------------------------------------------
// Edge: no PK at all (valid)
// ---------------------------------------------------------------------------

func TestParseTableWithoutPK(t *testing.T) {
	const schema = `
[database]
name = "testdb"

[[tables]]
name = "logs"

  [[tables.columns]]
  name = "message"
  type = "text"
`
	p := NewParser()
	db, err := p.Parse(strings.NewReader(schema))
	require.NoError(t, err)

	tbl := db.Tables[0]
	assert.Nil(t, tbl.PrimaryKey())
}

// ---------------------------------------------------------------------------
// Example schema file integration test
// ---------------------------------------------------------------------------

func TestParseFileExampleSchemaToml(t *testing.T) {
	p := NewParser()
	db, err := p.ParseFile(testdataPath("example_schema.toml"))
	require.NoError(t, err)
	require.NotNil(t, db)

	assert.Equal(t, "ecommerce", db.Name)
	assert.Equal(t, "mysql", db.Dialect)
	assert.Len(t, db.Tables, 4)

	// Verify all table names.
	want := []string{"tenants", "users", "roles", "user_roles"}
	for i, name := range want {
		assert.Equal(t, name, db.Tables[i].Name)
	}
}
