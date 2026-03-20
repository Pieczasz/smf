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

func TestPrimaryKeyConflictMultipleConstraints(t *testing.T) {
	db := &schema.Database{
		Name:    "app",
		Dialect: schema.DialectMySQL,
		Tables: []*schema.Table{
			{
				Name: "users",
				Columns: []*schema.Column{
					{Name: "id"},
				},
				Constraints: []*schema.Constraint{
					{Name: "pk1", Type: schema.ConstraintPrimaryKey, Columns: []string{"id"}},
					{Name: "pk2", Type: schema.ConstraintPrimaryKey, Columns: []string{"id"}},
				},
			},
		},
	}

	err := Database(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "multiple PRIMARY KEY constraints")
}

func TestPrimaryKeyConflictColumnAndConstraint(t *testing.T) {
	db := &schema.Database{
		Name:    "app",
		Dialect: schema.DialectMySQL,
		Tables: []*schema.Table{
			{
				Name: "users",
				Columns: []*schema.Column{
					{Name: "id", PrimaryKey: true},
				},
				Constraints: []*schema.Constraint{
					{Name: "pk_users", Type: schema.ConstraintPrimaryKey, Columns: []string{"id"}},
				},
			},
		},
	}

	err := Database(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "primary key declared on both")
}

func TestSynthesizeConstraints(t *testing.T) {
	db := &schema.Database{
		Name:    "app",
		Dialect: schema.DialectMySQL,
		Tables: []*schema.Table{
			{
				Name: "users",
				Columns: []*schema.Column{
					{Name: "id", Type: schema.DataTypeInt, PrimaryKey: true},
					{Name: "email", Type: schema.DataTypeString, Unique: true},
					{Name: "age", Type: schema.DataTypeInt, Check: "age >= 0"},
					{Name: "role_id", Type: schema.DataTypeInt, References: "roles.id", RefOnDelete: schema.RefActionCascade, RefOnUpdate: schema.RefActionRestrict},
				},
			},
			{
				Name: "roles",
				Columns: []*schema.Column{
					{Name: "id", Type: schema.DataTypeInt, PrimaryKey: true},
				},
			},
		},
	}

	err := Database(db)
	require.NoError(t, err)

	users := db.Tables[0]

	var uniqueCount, checkCount, fkCount int
	for _, c := range users.Constraints {
		switch c.Type {
		case schema.ConstraintUnique:
			uniqueCount++
		case schema.ConstraintCheck:
			checkCount++
		case schema.ConstraintForeignKey:
			fkCount++
			assert.Equal(t, "roles", c.ReferencedTable)
			assert.Equal(t, []string{"id"}, c.ReferencedColumns)
			assert.Equal(t, schema.RefActionCascade, c.OnDelete)
			assert.Equal(t, schema.RefActionRestrict, c.OnUpdate)
		}
	}
	assert.Equal(t, 1, uniqueCount)
	assert.Equal(t, 1, checkCount)
	assert.Equal(t, 1, fkCount)
}
