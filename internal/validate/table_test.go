package validate

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"smf/internal/schema"
)

func TestTableNoColumns(t *testing.T) {
	db := &schema.Database{
		Name:    "app",
		Dialect: schema.DialectMySQL,
		Tables: []*schema.Table{
			{Name: "users"},
		},
	}

	err := Database(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "table \"users\" has no columns")
}

func TestTableDuplicateColumnNames(t *testing.T) {
	db := &schema.Database{
		Name:    "app",
		Dialect: schema.DialectMySQL,
		Tables: []*schema.Table{
			{
				Name: "users",
				Columns: []*schema.Column{
					{Name: "email"},
					{Name: "email"},
				},
			},
		},
	}

	err := Database(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate column name")
}

func TestTableEmptyColumnName(t *testing.T) {
	db := &schema.Database{
		Name:    "app",
		Dialect: schema.DialectMySQL,
		Tables: []*schema.Table{
			{
				Name: "users",
				Columns: []*schema.Column{
					{Name: "   "},
				},
			},
		},
	}

	err := Database(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "name is empty")
}

func TestTableMaxColumnNameLength(t *testing.T) {
	db := &schema.Database{
		Name:    "app",
		Dialect: schema.DialectMySQL,
		Validation: &schema.ValidationRules{
			MaxColumnNameLength: 3,
		},
		Tables: []*schema.Table{
			{
				Name: "users",
				Columns: []*schema.Column{
					{Name: "email"},
				},
			},
		},
	}

	err := Database(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `exceeds maximum length 3`)
}

func TestTableAllowedNamePatternForColumn(t *testing.T) {
	db := &schema.Database{
		Name:    "app",
		Dialect: schema.DialectMySQL,
		Validation: &schema.ValidationRules{
			AllowedNamePattern: `^u[a-z]+$`,
		},
		Tables: []*schema.Table{
			{
				Name: "users",
				Columns: []*schema.Column{
					{Name: "email"},
				},
			},
		},
	}

	err := Database(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not match allowed pattern")
}



