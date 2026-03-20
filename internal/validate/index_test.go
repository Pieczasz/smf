package validate

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"smf/internal/schema"
)

func TestIndexDuplicateNames(t *testing.T) {
	db := &schema.Database{
		Name:    "app",
		Dialect: schema.DialectMySQL,
		Tables: []*schema.Table{
			{
				Name:    "users",
				Columns: []*schema.Column{{Name: "email", Type: schema.DataTypeString}},
				Indexes: []*schema.Index{
					{Name: "idx_email", Columns: []schema.ColumnIndex{{Name: "email"}}},
					{Name: "idx_email", Columns: []schema.ColumnIndex{{Name: "email"}}},
				},
			},
		},
	}

	err := Database(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate index name")
}

func TestIndexHasNoColumns(t *testing.T) {
	db := &schema.Database{
		Name:    "app",
		Dialect: schema.DialectMySQL,
		Tables: []*schema.Table{
			{
				Name:    "users",
				Columns: []*schema.Column{{Name: "email", Type: schema.DataTypeString}},
				Indexes: []*schema.Index{
					{Name: "idx_email"},
				},
			},
		},
	}

	err := Database(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "index idx_email has no columns")
}

func TestIndexUnnamedHasNoColumns(t *testing.T) {
	db := &schema.Database{
		Name:    "app",
		Dialect: schema.DialectMySQL,
		Tables: []*schema.Table{
			{
				Name:    "users",
				Columns: []*schema.Column{{Name: "email", Type: schema.DataTypeString}},
				Indexes: []*schema.Index{
					{},
				},
			},
		},
	}

	err := Database(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "index (unnamed) has no columns")
}

func TestIndexReferencesNonexistentColumn(t *testing.T) {
	db := &schema.Database{
		Name:    "app",
		Dialect: schema.DialectMySQL,
		Tables: []*schema.Table{
			{
				Name:    "users",
				Columns: []*schema.Column{{Name: "email", Type: schema.DataTypeString}},
				Indexes: []*schema.Index{
					{Name: "idx_missing", Columns: []schema.ColumnIndex{{Name: "missing"}}},
				},
			},
		},
	}

	err := Database(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `index "idx_missing" references nonexistent column "missing"`)
}

func TestIndexValid(t *testing.T) {
	db := &schema.Database{
		Name:    "app",
		Dialect: schema.DialectMySQL,
		Tables: []*schema.Table{
			{
				Name:    "users",
				Columns: []*schema.Column{{Name: "email", Type: schema.DataTypeString}},
				Indexes: []*schema.Index{
					{Name: "idx_email", Columns: []schema.ColumnIndex{{Name: "email"}}},
				},
			},
		},
	}

	err := Database(db)
	require.NoError(t, err)
}
