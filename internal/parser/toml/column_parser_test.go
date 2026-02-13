package toml

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"smf/internal/core"
)

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
	assert.Equal(t, "JSONB", col.RawType, "RawType should be the dialect-specific override")
	assert.Equal(t, core.DataTypeJSON, col.Type, "Type should be normalized from portable type")
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
	// raw_type is ignored when dialect is empty; RawType falls back to portable type.
	assert.Equal(t, "json", col.RawType)
	assert.Equal(t, core.DataTypeJSON, col.Type)
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
	assert.Equal(t, "int", col.RawType)
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
	assert.Equal(t, "enum('active','paused','deleted')", col.RawType)
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
	assert.Equal(t, "enum('it''s','they''re')", col.RawType, "single quotes in values should be escaped")
}

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

func TestParseMalformedReferencesNoDot(t *testing.T) {
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
  name = "tenant_id"
  type = "int"
  references = "tenants"
`
	p := NewParser()
	_, err := p.Parse(strings.NewReader(schema))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid references")
	assert.Contains(t, err.Error(), "tenants")
}

func TestParseMalformedReferencesDotAtEnd(t *testing.T) {
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
  name = "tenant_id"
  type = "int"
  references = "tenants."
`
	p := NewParser()
	_, err := p.Parse(strings.NewReader(schema))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid references")
}

func TestParseMalformedReferencesDotAtStart(t *testing.T) {
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
  name = "tenant_id"
  type = "int"
  references = ".id"
`
	p := NewParser()
	_, err := p.Parse(strings.NewReader(schema))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid references")
}

func TestParseValidReferencesStillWorks(t *testing.T) {
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
  name = "tenant_id"
  type = "int"
  references = "tenants.id"
`
	p := NewParser()
	db, err := p.Parse(strings.NewReader(schema))
	require.NoError(t, err)

	tbl := db.Tables[0]
	col := tbl.FindColumn("tenant_id")
	require.NotNil(t, col)
	assert.Equal(t, "tenants.id", col.References)

	// FK constraint should have been synthesized.
	var fk *core.Constraint
	for _, c := range tbl.Constraints {
		if c.Type == core.ConstraintForeignKey {
			fk = c
			break
		}
	}
	require.NotNil(t, fk)
	assert.Equal(t, "tenants", fk.ReferencedTable)
	assert.Equal(t, []string{"id"}, fk.ReferencedColumns)
}

func TestParseDuplicateColumnName(t *testing.T) {
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
  name = "id"
  type = "varchar(255)"
`
	p := NewParser()
	_, err := p.Parse(strings.NewReader(schema))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate column name")
	assert.Contains(t, err.Error(), "id")
}

func TestParseDuplicateColumnNameCaseInsensitive(t *testing.T) {
	const schema = `
[database]
name = "testdb"

[[tables]]
name = "items"

  [[tables.columns]]
  name = "Email"
  type = "varchar(255)"
  primary_key = true

  [[tables.columns]]
  name = "email"
  type = "text"
`
	p := NewParser()
	_, err := p.Parse(strings.NewReader(schema))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate column name")
}
