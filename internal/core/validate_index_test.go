package core

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateDatabaseIndexDuplicateNames(t *testing.T) {
	db := &Database{
		Name:    "app",
		Dialect: new(DialectMySQL),
		Tables: []*Table{
			{
				Name:    "users",
				Columns: []*Column{{Name: "email", Type: DataTypeString}},
				Indexes: []*Index{
					{Name: "idx_email", Columns: []ColumnIndex{{Name: "email"}}},
					{Name: "idx_email", Columns: []ColumnIndex{{Name: "email"}}},
				},
			},
		},
	}

	err := db.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate index name")
}

func TestValidateDatabaseIndexHasNoColumns(t *testing.T) {
	db := &Database{
		Name:    "app",
		Dialect: new(DialectMySQL),
		Tables: []*Table{
			{
				Name:    "users",
				Columns: []*Column{{Name: "email", Type: DataTypeString}},
				Indexes: []*Index{
					{Name: "idx_email"},
				},
			},
		},
	}

	err := db.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "index idx_email has no columns")
}

func TestValidateDatabaseUnnamedIndexHasNoColumns(t *testing.T) {
	db := &Database{
		Name:    "app",
		Dialect: new(DialectMySQL),
		Tables: []*Table{
			{
				Name:    "users",
				Columns: []*Column{{Name: "email", Type: DataTypeString}},
				Indexes: []*Index{
					{},
				},
			},
		},
	}

	err := db.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "index (unnamed) has no columns")
}

func TestValidateDatabaseIndexReferencesNonexistentColumn(t *testing.T) {
	db := &Database{
		Name:    "app",
		Dialect: new(DialectMySQL),
		Tables: []*Table{
			{
				Name:    "users",
				Columns: []*Column{{Name: "email", Type: DataTypeString}},
				Indexes: []*Index{
					{Name: "idx_missing", Columns: []ColumnIndex{{Name: "missing"}}},
				},
			},
		},
	}

	err := db.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), `index "idx_missing" references nonexistent column "missing"`)
}
