package core

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateDatabaseConstraintDuplicateNamesCaseInsensitive(t *testing.T) {
	db := &Database{
		Name:    "app",
		Dialect: new(DialectMySQL),
		Tables: []*Table{
			{
				Name:    "users",
				Columns: []*Column{{Name: "id"}},
				Constraints: []*Constraint{
					{Name: "uq_email", Type: ConstraintUnique, Columns: []string{"id"}},
					{Name: "UQ_EMAIL", Type: ConstraintUnique, Columns: []string{"id"}},
				},
			},
		},
	}

	err := ValidateDatabase(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate constraint name")
}

func TestValidateDatabaseConstraintWithNoColumns(t *testing.T) {
	db := &Database{
		Name:    "app",
		Dialect: new(DialectMySQL),
		Tables: []*Table{
			{
				Name:    "users",
				Columns: []*Column{{Name: "id"}},
				Constraints: []*Constraint{
					{Name: "uq_users_id", Type: ConstraintUnique},
				},
			},
		},
	}

	err := ValidateDatabase(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "has no columns")
}

func TestValidateDatabaseConstraintReferencesNonexistentColumn(t *testing.T) {
	db := &Database{
		Name:    "app",
		Dialect: new(DialectMySQL),
		Tables: []*Table{
			{
				Name:    "users",
				Columns: []*Column{{Name: "id"}},
				Constraints: []*Constraint{
					{Name: "uq_users_email", Type: ConstraintUnique, Columns: []string{"email"}},
				},
			},
		},
	}

	err := ValidateDatabase(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "references nonexistent column")
}

func TestValidateDatabaseConstraintForeignKeyMissingReferencedTable(t *testing.T) {
	db := &Database{
		Name:    "app",
		Dialect: new(DialectMySQL),
		Tables: []*Table{
			{
				Name:    "users",
				Columns: []*Column{{Name: "role_id"}},
				Constraints: []*Constraint{
					{Name: "fk_users_role", Type: ConstraintForeignKey, Columns: []string{"role_id"}},
				},
			},
		},
	}

	err := ValidateDatabase(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing referenced_table")
}

func TestValidateDatabaseConstraintForeignKeyMissingReferencedColumns(t *testing.T) {
	db := &Database{
		Name:    "app",
		Dialect: new(DialectMySQL),
		Tables: []*Table{
			{
				Name:    "users",
				Columns: []*Column{{Name: "role_id"}},
				Constraints: []*Constraint{
					{
						Name:            "fk_users_role",
						Type:            ConstraintForeignKey,
						Columns:         []string{"role_id"},
						ReferencedTable: "roles",
					},
				},
			},
		},
	}

	err := ValidateDatabase(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing referenced_columns")
}

func TestValidateDatabaseCheckConstraintMayHaveNoColumns(t *testing.T) {
	db := &Database{
		Name:    "app",
		Dialect: new(DialectMySQL),
		Tables: []*Table{
			{
				Name:    "users",
				Columns: []*Column{{Name: "age"}},
				Constraints: []*Constraint{
					{Name: "chk_age", Type: ConstraintCheck, CheckExpression: "age >= 0"},
				},
			},
		},
	}

	err := ValidateDatabase(db)
	require.NoError(t, err)
}

func TestValidateDatabaseForeignKeyTargetExistence(t *testing.T) {
	d := DialectMySQL

	t.Run("valid reference", func(t *testing.T) {
		db := &Database{
			Name:    "app",
			Dialect: &d,
			Tables: []*Table{
				{
					Name:    "users",
					Columns: []*Column{{Name: "id"}},
				},
				{
					Name:    "posts",
					Columns: []*Column{{Name: "author_id"}},
					Constraints: []*Constraint{
						{
							Name:              "fk_posts_author",
							Type:              ConstraintForeignKey,
							Columns:           []string{"author_id"},
							ReferencedTable:   "users",
							ReferencedColumns: []string{"id"},
						},
					},
				},
			},
		}
		require.NoError(t, ValidateDatabase(db))
	})

	t.Run("case-insensitive matching", func(t *testing.T) {
		db := &Database{
			Name:    "app",
			Dialect: &d,
			Tables: []*Table{
				{
					Name:    "Users",
					Columns: []*Column{{Name: "ID"}},
				},
				{
					Name:    "posts",
					Columns: []*Column{{Name: "author_id"}},
					Constraints: []*Constraint{
						{
							Name:              "fk_posts_author",
							Type:              ConstraintForeignKey,
							Columns:           []string{"author_id"},
							ReferencedTable:   "users",
							ReferencedColumns: []string{"id"},
						},
					},
				},
			},
		}
		require.NoError(t, ValidateDatabase(db))
	})
}

func TestValidateDatabaseForeignKeyNotExisting(t *testing.T) {
	d := DialectMySQL

	t.Run("non-existent table", func(t *testing.T) {
		db := &Database{
			Name:    "app",
			Dialect: &d,
			Tables: []*Table{
				{
					Name:    "posts",
					Columns: []*Column{{Name: "author_id"}},
					Constraints: []*Constraint{
						{
							Name:              "fk_posts_author",
							Type:              ConstraintForeignKey,
							Columns:           []string{"author_id"},
							ReferencedTable:   "users",
							ReferencedColumns: []string{"id"},
						},
					},
				},
			},
		}
		err := ValidateDatabase(db)
		require.Error(t, err)
		assert.Contains(t, err.Error(), `references non-existent table "users"`)
	})

	t.Run("non-existent column", func(t *testing.T) {
		db := &Database{
			Name:    "app",
			Dialect: &d,
			Tables: []*Table{
				{
					Name:    "users",
					Columns: []*Column{{Name: "id"}},
				},
				{
					Name:    "posts",
					Columns: []*Column{{Name: "author_id"}},
					Constraints: []*Constraint{
						{
							Name:              "fk_posts_author",
							Type:              ConstraintForeignKey,
							Columns:           []string{"author_id"},
							ReferencedTable:   "users",
							ReferencedColumns: []string{"uuid"},
						},
					},
				},
			},
		}
		err := ValidateDatabase(db)
		require.Error(t, err)
		assert.Contains(t, err.Error(), `references non-existent column "uuid" in table "users"`)
	})
}
