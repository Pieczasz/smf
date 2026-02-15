package core

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateDatabaseTableHasNoColumns(t *testing.T) {
	db := &Database{
		Name:    "app",
		Dialect: new(DialectMySQL),
		Tables: []*Table{
			{Name: "users"},
		},
	}

	err := ValidateDatabase(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "table has no columns")
}

func TestValidateDatabaseDuplicateColumnNamesCaseInsensitive(t *testing.T) {
	db := &Database{
		Name:    "app",
		Dialect: new(DialectMySQL),
		Tables: []*Table{
			{
				Name: "users",
				Columns: []*Column{
					{Name: "email"},
					{Name: "Email"},
				},
			},
		},
	}

	err := ValidateDatabase(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate column name")
}

func TestValidateDatabaseEmptyColumnName(t *testing.T) {
	db := &Database{
		Name:    "app",
		Dialect: new(DialectMySQL),
		Tables: []*Table{
			{
				Name: "users",
				Columns: []*Column{
					{Name: "   "},
				},
			},
		},
	}

	err := ValidateDatabase(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "column")
	assert.Contains(t, err.Error(), "name is empty")
}

func TestValidateDatabaseMaxColumnNameLength(t *testing.T) {
	db := &Database{
		Name:    "app",
		Dialect: new(DialectMySQL),
		Validation: &ValidationRules{
			MaxColumnNameLength: 3,
		},
		Tables: []*Table{
			{
				Name: "users",
				Columns: []*Column{
					{Name: "email"},
				},
			},
		},
	}

	err := ValidateDatabase(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `column "email" exceeds maximum length 3`)
}

func TestValidateDatabaseAllowedNamePatternForColumn(t *testing.T) {
	db := &Database{
		Name:    "app",
		Dialect: new(DialectMySQL),
		Validation: &ValidationRules{
			AllowedNamePattern: `^[a-z_]+$`,
		},
		Tables: []*Table{
			{
				Name: "users",
				Columns: []*Column{
					{Name: "Email"},
				},
			},
		},
	}

	err := ValidateDatabase(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not match allowed pattern")
}

func TestValidateDatabaseInvalidReferencesFormat(t *testing.T) {
	db := &Database{
		Name:    "app",
		Dialect: new(DialectMySQL),
		Tables: []*Table{
			{
				Name: "users",
				Columns: []*Column{
					{Name: "role_id", References: "roles"},
				},
			},
		},
	}

	err := ValidateDatabase(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `invalid references "roles"`)
}

func TestValidateDatabaseMultiplePrimaryKeyConstraints(t *testing.T) {
	db := &Database{
		Name:    "app",
		Dialect: new(DialectMySQL),
		Tables: []*Table{
			{
				Name: "users",
				Columns: []*Column{
					{Name: "id"},
				},
				Constraints: []*Constraint{
					{Name: "pk1", Type: ConstraintPrimaryKey, Columns: []string{"id"}},
					{Name: "pk2", Type: ConstraintPrimaryKey, Columns: []string{"id"}},
				},
			},
		},
	}

	err := ValidateDatabase(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "multiple PRIMARY KEY constraints")
}

func TestValidateDatabaseColumnAndConstraintPrimaryKeyBothPresent(t *testing.T) {
	db := &Database{
		Name:    "app",
		Dialect: new(DialectMySQL),
		Tables: []*Table{
			{
				Name: "users",
				Columns: []*Column{
					{Name: "id", PrimaryKey: true},
				},
				Constraints: []*Constraint{
					{Name: "pk_users", Type: ConstraintPrimaryKey, Columns: []string{"id"}},
				},
			},
		},
	}

	err := ValidateDatabase(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "primary key declared on both")
}
