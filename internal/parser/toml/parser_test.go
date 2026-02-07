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
	assert.Len(t, db.Tables, 4)

	want := []string{"tenants", "users", "roles", "user_roles"}
	for i, name := range want {
		assert.Equal(t, name, db.Tables[i].Name)
	}
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
	assert.Len(t, tbl.Columns, 7)

	t.Run("Columns", func(t *testing.T) {
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

		name := tbl.FindColumn("name")
		require.NotNil(t, name)
		assert.Equal(t, "varchar(255)", name.TypeRaw)

		plan := tbl.FindColumn("plan")
		require.NotNil(t, plan)
		assert.Equal(t, "enum('free','pro','enterprise')", plan.TypeRaw)
		assert.Equal(t, core.DataTypeString, plan.Type)
		require.NotNil(t, plan.DefaultValue)
		assert.Equal(t, "free", *plan.DefaultValue)

		settings := tbl.FindColumn("settings")
		require.NotNil(t, settings)
		assert.Equal(t, "json", settings.TypeRaw)
		assert.Equal(t, core.DataTypeJSON, settings.Type)
		assert.True(t, settings.Nullable)

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
	})

	t.Run("Constraints", func(t *testing.T) {
		assert.Len(t, tbl.Constraints, 2)

		pk := tbl.PrimaryKey()
		require.NotNil(t, pk)
		assert.Equal(t, core.ConstraintPrimaryKey, pk.Type)
		assert.Equal(t, []string{"id"}, pk.Columns)

		uq := tbl.FindConstraint("uq_tenants_slug")
		require.NotNil(t, uq)
		assert.Equal(t, core.ConstraintUnique, uq.Type)
		assert.Equal(t, []string{"slug"}, uq.Columns)
	})
}

func TestParseFileUsers(t *testing.T) {
	p := NewParser()
	db, err := p.ParseFile(testdataPath("schema.toml"))
	require.NoError(t, err)

	tbl := db.FindTable("users")
	require.NotNil(t, tbl)

	assert.Equal(t, "Application user", tbl.Comment)
	assert.Len(t, tbl.Columns, 8)

	pwHash := tbl.FindColumn("password_hash")
	require.NotNil(t, pwHash)
	assert.Equal(t, "varbinary(60)", pwHash.TypeRaw)
	assert.Equal(t, core.DataTypeBinary, pwHash.Type)

	displayName := tbl.FindColumn("display_name")
	require.NotNil(t, displayName)
	assert.True(t, displayName.Nullable)

	isActive := tbl.FindColumn("is_active")
	require.NotNil(t, isActive)
	assert.Equal(t, "boolean", isActive.TypeRaw)
	assert.Equal(t, core.DataTypeBoolean, isActive.Type)
	require.NotNil(t, isActive.DefaultValue)
	assert.Equal(t, "1", *isActive.DefaultValue)

	assert.Len(t, tbl.Constraints, 4)

	fk := tbl.FindConstraint("fk_users_tenant")
	require.NotNil(t, fk)
	assert.Equal(t, core.ConstraintForeignKey, fk.Type)
	assert.Equal(t, []string{"tenant_id"}, fk.Columns)
	assert.Equal(t, "tenants", fk.ReferencedTable)
	assert.Equal(t, []string{"id"}, fk.ReferencedColumns)
	assert.Equal(t, core.RefActionCascade, fk.OnDelete)
	assert.Equal(t, core.RefActionRestrict, fk.OnUpdate)

	chk := tbl.FindConstraint("chk_users_email")
	require.NotNil(t, chk)
	assert.Equal(t, core.ConstraintCheck, chk.Type)
	assert.Equal(t, "email LIKE '%@%'", chk.CheckExpression)

	assert.Len(t, tbl.Indexes, 1)
	idx := tbl.FindIndex("idx_users_tenant")
	require.NotNil(t, idx)
	assert.Equal(t, core.IndexTypeBTree, idx.Type)
	assert.Equal(t, core.IndexVisible, idx.Visibility)
	require.Len(t, idx.Columns, 1)
	assert.Equal(t, "tenant_id", idx.Columns[0].Name)
}

func TestParseFileRoles(t *testing.T) {
	p := NewParser()
	db, err := p.ParseFile(testdataPath("schema.toml"))
	require.NoError(t, err)

	tbl := db.FindTable("roles")
	require.NotNil(t, tbl)

	assert.Equal(t, "RBAC role", tbl.Comment)
	assert.Len(t, tbl.Columns, 5)

	desc := tbl.FindColumn("description")
	require.NotNil(t, desc)
	assert.True(t, desc.Nullable)

	assert.Len(t, tbl.Constraints, 3)

	uq := tbl.FindConstraint("uq_roles_tenant_name")
	require.NotNil(t, uq)
	assert.Equal(t, core.ConstraintUnique, uq.Type)
	assert.Equal(t, []string{"tenant_id", "name"}, uq.Columns)

	fk := tbl.FindConstraint("fk_roles_tenant")
	require.NotNil(t, fk)
	assert.Equal(t, core.ConstraintForeignKey, fk.Type)
	assert.Equal(t, "tenants", fk.ReferencedTable)
}

func TestParseFileUserRoles(t *testing.T) {
	p := NewParser()
	db, err := p.ParseFile(testdataPath("schema.toml"))
	require.NoError(t, err)

	tbl := db.FindTable("user_roles")
	require.NotNil(t, tbl)

	assert.Equal(t, "RBAC role assignments (many-to-many)", tbl.Comment)
	assert.Len(t, tbl.Columns, 3)

	pk := tbl.PrimaryKey()
	require.NotNil(t, pk)
	assert.Equal(t, []string{"user_id", "role_id"}, pk.Columns)

	fkUser := tbl.FindConstraint("fk_user_roles_user")
	require.NotNil(t, fkUser)
	assert.Equal(t, "users", fkUser.ReferencedTable)

	fkRole := tbl.FindConstraint("fk_user_roles_role")
	require.NotNil(t, fkRole)
	assert.Equal(t, "roles", fkRole.ReferencedTable)

	assert.Len(t, tbl.Indexes, 1)
	idx := tbl.FindIndex("idx_user_roles_role")
	require.NotNil(t, idx)
	require.Len(t, idx.Columns, 1)
	assert.Equal(t, "role_id", idx.Columns[0].Name)
}

func TestParseMinimalSchema(t *testing.T) {
	const schema = `
[database]
name = "testdb"

[[database.tables]]
name = "items"

  [[database.tables.columns]]
  name = "id"
  type = "int"
  primary_key = true
  auto_increment = true

  [[database.tables.columns]]
  name = "label"
  type = "varchar(100)"

  [[database.tables.constraints]]
  type    = "PRIMARY KEY"
  columns = ["id"]
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
	assert.Len(t, tbl.Constraints, 1)

	assert.True(t, tbl.Columns[0].PrimaryKey)
	assert.Equal(t, core.DataTypeInt, tbl.Columns[0].Type)
}

func TestParseTypeRawOverridesType(t *testing.T) {
	const schema = `
[database]
name = "testdb"

[[database.tables]]
name = "items"

  [[database.tables.columns]]
  name     = "data"
  type     = "json"
  type_raw = "JSONB"

  [[database.tables.constraints]]
  type    = "PRIMARY KEY"
  columns = ["data"]
`
	p := NewParser()
	db, err := p.Parse(strings.NewReader(schema))
	require.NoError(t, err)

	col := db.Tables[0].FindColumn("data")
	require.NotNil(t, col)
	assert.Equal(t, "JSONB", col.TypeRaw)
}

func TestParseNullableDefaultFalse(t *testing.T) {
	const schema = `
[database]
name = "testdb"

[[database.tables]]
name = "items"

  [[database.tables.columns]]
  name = "id"
  type = "int"

  [[database.tables.columns]]
  name     = "note"
  type     = "text"
  nullable = true

  [[database.tables.constraints]]
  type    = "PRIMARY KEY"
  columns = ["id"]
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

[[database.tables]]
name = "items"

  [[database.tables.columns]]
  name = "id"
  type = "int"

  [[database.tables.constraints]]
  type    = "PRIMARY KEY"
  columns = ["id"]
`
	p := NewParser()
	db, err := p.Parse(strings.NewReader(schema))
	require.NoError(t, err)

	col := db.Tables[0].Columns[0]
	assert.Nil(t, col.DefaultValue)
	assert.Nil(t, col.OnUpdate)
}

func TestParseDefaultValueAndOnUpdate(t *testing.T) {
	const schema = `
[database]
name = "testdb"

[[database.tables]]
name = "items"

  [[database.tables.columns]]
  name          = "created_at"
  type          = "timestamp"
  default_value = "CURRENT_TIMESTAMP"
  on_update     = "CURRENT_TIMESTAMP"

  [[database.tables.constraints]]
  type    = "PRIMARY KEY"
  columns = ["created_at"]
`
	p := NewParser()
	db, err := p.Parse(strings.NewReader(schema))
	require.NoError(t, err)

	col := db.Tables[0].Columns[0]
	require.NotNil(t, col.DefaultValue)
	assert.Equal(t, "CURRENT_TIMESTAMP", *col.DefaultValue)
	require.NotNil(t, col.OnUpdate)
	assert.Equal(t, "CURRENT_TIMESTAMP", *col.OnUpdate)
}

func TestParseConstraintEnforcedDefault(t *testing.T) {
	const schema = `
[database]
name = "testdb"

[[database.tables]]
name = "items"

  [[database.tables.columns]]
  name = "val"
  type = "int"

  [[database.tables.constraints]]
  type             = "CHECK"
  check_expression = "val > 0"

  [[database.tables.constraints]]
  type             = "CHECK"
  check_expression = "val < 1000"
  enforced         = false
`
	p := NewParser()
	db, err := p.Parse(strings.NewReader(schema))
	require.NoError(t, err)

	require.Len(t, db.Tables[0].Constraints, 2)
	assert.True(t, db.Tables[0].Constraints[0].Enforced)
	assert.False(t, db.Tables[0].Constraints[1].Enforced)
}

func TestParseIndexDefaults(t *testing.T) {
	const schema = `
[database]
name = "testdb"

[[database.tables]]
name = "items"

  [[database.tables.columns]]
  name = "id"
  type = "int"

  [[database.tables.constraints]]
  type    = "PRIMARY KEY"
  columns = ["id"]

  [[database.tables.indexes]]
  name = "idx_items_id"

    [[database.tables.indexes.columns]]
    name = "id"
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

func TestParseGeneratedColumn(t *testing.T) {
	const schema = `
[database]
name = "testdb"

[[database.tables]]
name = "items"

  [[database.tables.columns]]
  name                  = "id"
  type                  = "int"

  [[database.tables.columns]]
  name                  = "full_name"
  type                  = "varchar(255)"
  is_generated          = true
  generation_expression = "CONCAT(first_name, ' ', last_name)"
  generation_storage    = "STORED"

  [[database.tables.constraints]]
  type    = "PRIMARY KEY"
  columns = ["id"]
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

[[database.tables]]
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

[[database.tables]]
name = "items"

  [[database.tables.columns]]
  name = ""
  type = "int"

  [[database.tables.constraints]]
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

[[database.tables]]
name = "items"

  [[database.tables.columns]]
  name = "id"

  [[database.tables.constraints]]
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

[[database.tables]]
name = "items"

  [[database.tables.columns]]
  name = "id"
  type = "int"

  [[database.tables.constraints]]
  type    = "PRIMARY KEY"
  columns = ["id"]

[[database.tables]]
name = "items"

  [[database.tables.columns]]
  name = "id"
  type = "int"

  [[database.tables.constraints]]
  type    = "PRIMARY KEY"
  columns = ["id"]
`
	p := NewParser()
	_, err := p.Parse(strings.NewReader(schema))
	assert.Error(t, err)
}

func TestParseDataTypeNormalization(t *testing.T) {
	tests := []struct {
		rawType  string
		expected core.DataType
	}{
		{"varchar(255)", core.DataTypeString},
		{"char(10)", core.DataTypeString},
		{"text", core.DataTypeString},
		{"enum('a','b')", core.DataTypeString},
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

[[database.tables]]
name = "items"

  [[database.tables.columns]]
  name = "col"
  type = "` + tt.rawType + `"

  [[database.tables.constraints]]
  type    = "PRIMARY KEY"
  columns = ["col"]
`
			p := NewParser()
			db, err := p.Parse(strings.NewReader(schema))
			require.NoError(t, err)

			col := db.Tables[0].Columns[0]
			assert.Equal(t, tt.expected, col.Type)
		})
	}
}

func TestParseForeignKeyConstraint(t *testing.T) {
	const schema = `
[database]
name = "testdb"

[[database.tables]]
name = "parents"

  [[database.tables.columns]]
  name = "id"
  type = "int"

  [[database.tables.constraints]]
  type    = "PRIMARY KEY"
  columns = ["id"]

[[database.tables]]
name = "children"

  [[database.tables.columns]]
  name = "id"
  type = "int"

  [[database.tables.columns]]
  name = "parent_id"
  type = "int"

  [[database.tables.constraints]]
  type    = "PRIMARY KEY"
  columns = ["id"]

  [[database.tables.constraints]]
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

func TestParseIndexExplicitOptions(t *testing.T) {
	const schema = `
[database]
name = "testdb"

[[database.tables]]
name = "items"

  [[database.tables.columns]]
  name = "id"
  type = "int"

  [[database.tables.columns]]
  name = "label"
  type = "varchar(100)"

  [[database.tables.constraints]]
  type    = "PRIMARY KEY"
  columns = ["id"]

  [[database.tables.indexes]]
  name       = "idx_items_label"
  unique     = true
  type       = "HASH"
  visibility = "INVISIBLE"
  comment    = "fast label lookup"

    [[database.tables.indexes.columns]]
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

func TestParseTableOptions(t *testing.T) {
	const schema = `
[database]
name = "testdb"

[[database.tables]]
name = "items"

  [database.tables.options]
  engine       = "InnoDB"
  charset      = "utf8mb4"
  collate      = "utf8mb4_general_ci"
  row_format   = "COMPRESSED"
  tablespace   = "ts1"
  compression  = "zlib"
  encryption   = "Y"
  key_block_size = 8

  [[database.tables.columns]]
  name = "id"
  type = "int"

  [[database.tables.constraints]]
  type    = "PRIMARY KEY"
  columns = ["id"]
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
