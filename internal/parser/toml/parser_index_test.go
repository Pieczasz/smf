package toml

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"smf/internal/core"
)

func TestParseIndexSimpleColumns(t *testing.T) {
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
dialect = "mysql"

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
dialect = "mysql"

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

func TestParseIndexEmptyColumns(t *testing.T) {
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

  [[tables.indexes]]
  name = "idx_empty"
`
	p := NewParser()
	_, err := p.Parse(strings.NewReader(schema))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no columns")
}

func TestParseIndexEmptyColumnsUnnamed(t *testing.T) {
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

  [[tables.indexes]]
  unique = true
`
	p := NewParser()
	_, err := p.Parse(strings.NewReader(schema))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no columns")
}

func TestParseDuplicateIndexName(t *testing.T) {
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

  [[tables.columns]]
  name = "code"
  type = "varchar(50)"

  [[tables.columns]]
  name = "name"
  type = "varchar(100)"

  [[tables.indexes]]
  name    = "idx_code"
  columns = ["code"]

  [[tables.indexes]]
  name    = "idx_code"
  columns = ["name"]
`
	p := NewParser()
	_, err := p.Parse(strings.NewReader(schema))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate index name")
	assert.Contains(t, err.Error(), "idx_code")
}

func TestParseDuplicateIndexNameCaseInsensitive(t *testing.T) {
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

  [[tables.columns]]
  name = "code"
  type = "varchar(50)"

  [[tables.columns]]
  name = "name"
  type = "varchar(100)"

  [[tables.indexes]]
  name    = "IDX_CODE"
  columns = ["code"]

  [[tables.indexes]]
  name    = "idx_code"
  columns = ["name"]
`
	p := NewParser()
	_, err := p.Parse(strings.NewReader(schema))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate index name")
}

func TestParseIndexReferencesNonexistentColumn(t *testing.T) {
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

  [[tables.indexes]]
  name    = "idx_ghost"
  columns = ["nonexistent"]
`
	p := NewParser()
	_, err := p.Parse(strings.NewReader(schema))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nonexistent column")
	assert.Contains(t, err.Error(), "nonexistent")
}

func TestParseIndexAdvancedColumnDefsNonexistentColumn(t *testing.T) {
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

  [[tables.indexes]]
  name = "idx_ghost"

    [[tables.indexes.column_defs]]
    name  = "nonexistent"
    order = "ASC"
`
	p := NewParser()
	_, err := p.Parse(strings.NewReader(schema))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nonexistent column")
}

func TestParseIndexColumnDefWithoutOrder(t *testing.T) {
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

  [[tables.columns]]
  name = "label"
  type = "varchar(100)"

  [[tables.indexes]]
  name = "idx_label"

    [[tables.indexes.column_defs]]
    name   = "label"
    length = 10
`
	p := NewParser()
	db, err := p.Parse(strings.NewReader(schema))
	require.NoError(t, err)

	idx := db.Tables[0].FindIndex("idx_label")
	require.NotNil(t, idx)
	require.Len(t, idx.Columns, 1)
	assert.Equal(t, "label", idx.Columns[0].Name)
	assert.Equal(t, 10, idx.Columns[0].Length)
	assert.Equal(t, core.SortAsc, idx.Columns[0].Order, "order should default to ASC when omitted")
}

func TestParseUnnamedIndexValid(t *testing.T) {
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

  [[tables.columns]]
  name = "code"
  type = "varchar(50)"

  [[tables.indexes]]
  columns = ["code"]
`
	p := NewParser()
	db, err := p.Parse(strings.NewReader(schema))
	require.NoError(t, err)

	require.Len(t, db.Tables[0].Indexes, 1)
	idx := db.Tables[0].Indexes[0]
	assert.Empty(t, idx.Name)
	require.Len(t, idx.Columns, 1)
	assert.Equal(t, "code", idx.Columns[0].Name)
}

func TestParseMultipleIndexesOneUnnamed(t *testing.T) {
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

  [[tables.columns]]
  name = "code"
  type = "varchar(50)"

  [[tables.columns]]
  name = "name"
  type = "varchar(100)"

  [[tables.indexes]]
  name    = "idx_code"
  columns = ["code"]

  [[tables.indexes]]
  columns = ["name"]
`
	p := NewParser()
	db, err := p.Parse(strings.NewReader(schema))
	require.NoError(t, err)

	require.Len(t, db.Tables[0].Indexes, 2)
	assert.Equal(t, "idx_code", db.Tables[0].Indexes[0].Name)
	assert.Empty(t, db.Tables[0].Indexes[1].Name)
}

func TestParseColumnIndexesExistValid(t *testing.T) {
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

  [[tables.columns]]
  name = "code"
  type = "varchar(50)"

  [[tables.indexes]]
  name    = "idx_code"
  columns = ["code"]
`
	p := NewParser()
	db, err := p.Parse(strings.NewReader(schema))
	require.NoError(t, err)
	assert.Len(t, db.Tables[0].Indexes, 1)
	assert.Equal(t, "code", db.Tables[0].Indexes[0].Columns[0].Name)
}
