package validate

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"smf/internal/schema"
)

func TestConstraintDuplicateNames(t *testing.T) {
	db := &schema.Database{
		Name:    "app",
		Dialect: schema.DialectMySQL,
		Tables: []*schema.Table{
			{
				Name:    "users",
				Columns: []*schema.Column{{Name: "id", Type: schema.DataTypeInt}},
				Constraints: []*schema.Constraint{
					{Name: "uq_email", Type: schema.ConstraintUnique, Columns: []string{"id"}},
					{Name: "uq_email", Type: schema.ConstraintUnique, Columns: []string{"id"}},
				},
			},
		},
	}

	err := Database(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate constraint name")
}

func TestConstraintWithNoColumns(t *testing.T) {
	db := &schema.Database{
		Name:    "app",
		Dialect: schema.DialectMySQL,
		Tables: []*schema.Table{
			{
				Name:    "users",
				Columns: []*schema.Column{{Name: "id", Type: schema.DataTypeInt}},
				Constraints: []*schema.Constraint{
					{Name: "uq_users_id", Type: schema.ConstraintUnique, Columns: []string{}},
				},
			},
		},
	}

	err := Database(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "has no columns")
}

func TestConstraintReferencesNonexistentColumn(t *testing.T) {
	db := &schema.Database{
		Name:    "app",
		Dialect: schema.DialectMySQL,
		Tables: []*schema.Table{
			{
				Name:    "users",
				Columns: []*schema.Column{{Name: "id", Type: schema.DataTypeInt}},
				Constraints: []*schema.Constraint{
					{Name: "uq_users_email", Type: schema.ConstraintUnique, Columns: []string{"email"}},
				},
			},
		},
	}

	err := Database(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "references nonexistent column")
}

func TestConstraintForeignKeyMissingReferencedTable(t *testing.T) {
	db := &schema.Database{
		Name:    "app",
		Dialect: schema.DialectMySQL,
		Tables: []*schema.Table{
			{
				Name:    "users",
				Columns: []*schema.Column{{Name: "role_id", Type: schema.DataTypeInt}},
				Constraints: []*schema.Constraint{
					{Name: "fk_users_role", Type: schema.ConstraintForeignKey, Columns: []string{"role_id"}},
				},
			},
		},
	}

	err := Database(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing referenced_table")
}

func TestConstraintForeignKeyMissingReferencedColumns(t *testing.T) {
	db := &schema.Database{
		Name:    "app",
		Dialect: schema.DialectMySQL,
		Tables: []*schema.Table{
			{
				Name:    "users",
				Columns: []*schema.Column{{Name: "role_id", Type: schema.DataTypeInt}},
				Constraints: []*schema.Constraint{
					{
						Name:            "fk_users_role",
						Type:            schema.ConstraintForeignKey,
						Columns:         []string{"role_id"},
						ReferencedTable: "roles",
					},
				},
			},
		},
	}

	err := Database(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing referenced_columns")
}

func TestConstraintCheckMayHaveNoColumns(t *testing.T) {
	db := &schema.Database{
		Name:    "app",
		Dialect: schema.DialectMySQL,
		Tables: []*schema.Table{
			{
				Name:    "users",
				Columns: []*schema.Column{{Name: "age", Type: schema.DataTypeInt}},
				Constraints: []*schema.Constraint{
					{Name: "chk_age", Type: schema.ConstraintCheck, CheckExpression: "age >= 0"},
				},
			},
		},
	}

	err := Database(db)
	require.NoError(t, err)
}

func TestForeignKeyValidReference(t *testing.T) {
	db := &schema.Database{
		Name:    "app",
		Dialect: schema.DialectMySQL,
		Tables: []*schema.Table{
			{
				Name:    "users",
				Columns: []*schema.Column{{Name: "id", Type: schema.DataTypeInt}},
			},
			{
				Name:    "posts",
				Columns: []*schema.Column{{Name: "author_id", Type: schema.DataTypeInt}},
				Constraints: []*schema.Constraint{
					{
						Name:              "fk_posts_author",
						Type:              schema.ConstraintForeignKey,
						Columns:           []string{"author_id"},
						ReferencedTable:   "users",
						ReferencedColumns: []string{"id"},
					},
				},
			},
		},
	}

	err := Database(db)
	require.NoError(t, err)
}

func TestForeignKeyNonExistentTable(t *testing.T) {
	db := &schema.Database{
		Name:    "app",
		Dialect: schema.DialectMySQL,
		Tables: []*schema.Table{
			{
				Name:    "posts",
				Columns: []*schema.Column{{Name: "author_id", Type: schema.DataTypeInt}},
				Constraints: []*schema.Constraint{
					{
						Name:              "fk_posts_author",
						Type:              schema.ConstraintForeignKey,
						Columns:           []string{"author_id"},
						ReferencedTable:   "users",
						ReferencedColumns: []string{"id"},
					},
				},
			},
		},
	}

	err := Database(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `references non-existent table "users"`)
}

func TestForeignKeyNonExistentColumn(t *testing.T) {
	db := &schema.Database{
		Name:    "app",
		Dialect: schema.DialectMySQL,
		Tables: []*schema.Table{
			{
				Name:    "users",
				Columns: []*schema.Column{{Name: "id", Type: schema.DataTypeInt}},
			},
			{
				Name:    "posts",
				Columns: []*schema.Column{{Name: "author_id", Type: schema.DataTypeInt}},
				Constraints: []*schema.Constraint{
					{
						Name:              "fk_posts_author",
						Type:              schema.ConstraintForeignKey,
						Columns:           []string{"author_id"},
						ReferencedTable:   "users",
						ReferencedColumns: []string{"uuid"},
					},
				},
			},
		},
	}

	err := Database(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `references non-existent column "uuid" in table "users"`)
}
