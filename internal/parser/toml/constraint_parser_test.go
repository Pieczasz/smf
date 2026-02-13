package toml

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"smf/internal/core"
)

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

func TestParseMultiplePKConstraints(t *testing.T) {
	const schema = `
[database]
name = "testdb"

[[tables]]
name = "items"

  [[tables.columns]]
  name = "id"
  type = "int"

  [[tables.columns]]
  name = "code"
  type = "varchar(50)"

  [[tables.constraints]]
  type    = "PRIMARY KEY"
  columns = ["id"]

  [[tables.constraints]]
  type    = "PRIMARY KEY"
  columns = ["code"]
`
	p := NewParser()
	_, err := p.Parse(strings.NewReader(schema))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "multiple PRIMARY KEY")
}

func TestParseDuplicateConstraintName(t *testing.T) {
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
  name = "code"
  type = "varchar(50)"

  [[tables.columns]]
  name = "name"
  type = "varchar(100)"

  [[tables.constraints]]
  name    = "uq_code"
  type    = "UNIQUE"
  columns = ["code"]

  [[tables.constraints]]
  name    = "uq_code"
  type    = "UNIQUE"
  columns = ["name"]
`
	p := NewParser()
	_, err := p.Parse(strings.NewReader(schema))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate constraint name")
	assert.Contains(t, err.Error(), "uq_code")
}

func TestParseDuplicateConstraintNameCaseInsensitive(t *testing.T) {
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
  name = "code"
  type = "varchar(50)"

  [[tables.columns]]
  name = "name"
  type = "varchar(100)"

  [[tables.constraints]]
  name    = "UQ_CODE"
  type    = "UNIQUE"
  columns = ["code"]

  [[tables.constraints]]
  name    = "uq_code"
  type    = "UNIQUE"
  columns = ["name"]
`
	p := NewParser()
	_, err := p.Parse(strings.NewReader(schema))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate constraint name")
}

func TestParseConstraintReferencesNonexistentColumn(t *testing.T) {
	const schema = `
[database]
name = "testdb"

[[tables]]
name = "items"

  [[tables.columns]]
  name = "id"
  type = "int"
  primary_key = true

  [[tables.constraints]]
  name    = "uq_ghost"
  type    = "UNIQUE"
  columns = ["nonexistent"]
`
	p := NewParser()
	_, err := p.Parse(strings.NewReader(schema))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nonexistent column")
	assert.Contains(t, err.Error(), "nonexistent")
}

func TestParseConstraintEmptyColumns(t *testing.T) {
	const schema = `
[database]
name = "testdb"

[[tables]]
name = "items"

  [[tables.columns]]
  name = "id"
  type = "int"
  primary_key = true

  [[tables.constraints]]
  name = "uq_empty"
  type = "UNIQUE"
`
	p := NewParser()
	_, err := p.Parse(strings.NewReader(schema))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "has no columns")
}

func TestParseFKConstraintMissingReferencedTable(t *testing.T) {
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

  [[tables.constraints]]
  name    = "fk_tenant"
  type    = "FOREIGN KEY"
  columns = ["tenant_id"]
  referenced_columns = ["id"]
`
	p := NewParser()
	_, err := p.Parse(strings.NewReader(schema))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing referenced_table")
}

func TestParseFKConstraintMissingReferencedColumns(t *testing.T) {
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

  [[tables.constraints]]
  name             = "fk_tenant"
  type             = "FOREIGN KEY"
  columns          = ["tenant_id"]
  referenced_table = "tenants"
`
	p := NewParser()
	_, err := p.Parse(strings.NewReader(schema))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing referenced_columns")
}

func TestParseConstraintColumnsExistValid(t *testing.T) {
	const schema = `
[database]
name = "testdb"

[[tables]]
name = "items"

  [[tables.columns]]
  name = "id"
  type = "int"

  [[tables.columns]]
  name = "code"
  type = "varchar(50)"

  [[tables.constraints]]
  type    = "PRIMARY KEY"
  columns = ["id"]

  [[tables.constraints]]
  name    = "uq_code"
  type    = "UNIQUE"
  columns = ["code"]
`
	p := NewParser()
	db, err := p.Parse(strings.NewReader(schema))
	require.NoError(t, err)
	assert.Len(t, db.Tables[0].Constraints, 2)
}

func TestParseExplicitFKConstraintValid(t *testing.T) {
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

  [[tables.constraints]]
  name               = "fk_tenant"
  type               = "FOREIGN KEY"
  columns            = ["tenant_id"]
  referenced_table   = "tenants"
  referenced_columns = ["id"]
  on_delete          = "CASCADE"
`
	p := NewParser()
	db, err := p.Parse(strings.NewReader(schema))
	require.NoError(t, err)

	tbl := db.Tables[0]
	fk := tbl.FindConstraint("fk_tenant")
	require.NotNil(t, fk)
	assert.Equal(t, core.ConstraintForeignKey, fk.Type)
	assert.Equal(t, "tenants", fk.ReferencedTable)
	assert.Equal(t, []string{"id"}, fk.ReferencedColumns)
	assert.Equal(t, core.RefActionCascade, fk.OnDelete)
}

func TestParseCheckConstraintWithoutColumnsValid(t *testing.T) {
	// CHECK constraints use expressions, not column lists, so they should
	// pass even with an empty columns list.
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
  name = "price"
  type = "decimal(10,2)"

  [[tables.constraints]]
  name             = "chk_price"
  type             = "CHECK"
  check_expression = "price > 0"
`
	p := NewParser()
	db, err := p.Parse(strings.NewReader(schema))
	require.NoError(t, err)

	tbl := db.Tables[0]
	chk := tbl.FindConstraint("chk_price")
	require.NotNil(t, chk)
	assert.Equal(t, core.ConstraintCheck, chk.Type)
	assert.Equal(t, "price > 0", chk.CheckExpression)
}

func TestParsePKConstraintReferencesNonexistentColumn(t *testing.T) {
	const schema = `
[database]
name = "testdb"

[[tables]]
name = "items"

  [[tables.columns]]
  name = "id"
  type = "int"

  [[tables.constraints]]
  type    = "PRIMARY KEY"
  columns = ["missing_col"]
`
	p := NewParser()
	_, err := p.Parse(strings.NewReader(schema))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nonexistent column")
	assert.Contains(t, err.Error(), "missing_col")
}

func TestParseCompositePKConstraintOneColumnMissing(t *testing.T) {
	const schema = `
[database]
name = "testdb"

[[tables]]
name = "items"

  [[tables.columns]]
  name = "tenant_id"
  type = "int"

  [[tables.columns]]
  name = "item_id"
  type = "int"

  [[tables.constraints]]
  type    = "PRIMARY KEY"
  columns = ["tenant_id", "ghost"]
`
	p := NewParser()
	_, err := p.Parse(strings.NewReader(schema))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nonexistent column")
	assert.Contains(t, err.Error(), "ghost")
}
