package core

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateDatabaseSuccessAndSynthesis(t *testing.T) {
	db := &Database{
		Name:    "app",
		Dialect: new(DialectMySQL),
		Tables: []*Table{
			{
				Name: "users",
				Columns: []*Column{
					{Name: "id", PrimaryKey: true},
					{Name: "email", Unique: true},
					{Name: "age", Check: "age >= 0"},
					{Name: "role_id", References: "roles.id", RefOnDelete: "CASCADE", RefOnUpdate: "RESTRICT"},
				},
			},
			{
				Name: "roles",
				Columns: []*Column{
					{Name: "id", PrimaryKey: true},
				},
			},
		},
	}

	err := ValidateDatabase(db)
	require.NoError(t, err)

	users := db.Tables[0]
	require.NotNil(t, users.PrimaryKey())

	var uniqueCount, checkCount, fkCount int
	for _, c := range users.Constraints {
		switch c.Type {
		case ConstraintUnique:
			uniqueCount++
		case ConstraintCheck:
			checkCount++
		case ConstraintForeignKey:
			fkCount++
			assert.Equal(t, "roles", c.ReferencedTable)
			assert.Equal(t, []string{"id"}, c.ReferencedColumns)
			assert.Equal(t, ReferentialAction("CASCADE"), c.OnDelete)
			assert.Equal(t, ReferentialAction("RESTRICT"), c.OnUpdate)
		}
	}
	assert.Equal(t, 1, uniqueCount)
	assert.Equal(t, 1, checkCount)
	assert.Equal(t, 1, fkCount)
}

func TestValidateDatabaseNilDatabase(t *testing.T) {
	err := ValidateDatabase(nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "database is nil")
}

func TestValidateDatabaseMissingDialect(t *testing.T) {
	db := &Database{
		Name: "app",
		Tables: []*Table{
			{Name: "users", Columns: []*Column{{Name: "id"}}},
		},
	}

	err := ValidateDatabase(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "dialect is required")
}

func TestValidateDatabaseInvalidAllowedNamePattern(t *testing.T) {
	db := &Database{
		Name:    "app",
		Dialect: new(DialectMySQL),
		Validation: &ValidationRules{
			AllowedNamePattern: "(",
		},
		Tables: []*Table{
			{Name: "users", Columns: []*Column{{Name: "id"}}},
		},
	}

	err := ValidateDatabase(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid allowed_name_pattern")
}

func TestValidateDatabaseDuplicateTableNamesCaseInsensitive(t *testing.T) {
	db := &Database{
		Name:    "app",
		Dialect: new(DialectMySQL),
		Tables: []*Table{
			{Name: "users", Columns: []*Column{{Name: "id"}}},
			{Name: "Users", Columns: []*Column{{Name: "id"}}},
		},
	}

	err := ValidateDatabase(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate table name")
}

func TestValidateDatabaseErrorPrefixIncludesTableName(t *testing.T) {
	db := &Database{
		Name:    "app",
		Dialect: new(DialectMySQL),
		Tables: []*Table{
			{
				Name: "users",
				Columns: []*Column{
					{Name: "id"},
					{Name: "id"},
				},
			},
		},
	}

	err := ValidateDatabase(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `table "users":`)
}

func TestValidateDatabaseMissingName(t *testing.T) {
	db := &Database{
		Name:    "   ",
		Dialect: new(DialectMySQL),
		Tables: []*Table{
			{Name: "users", Columns: []*Column{{Name: "id"}}},
		},
	}

	err := ValidateDatabase(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "database name is required")
}

func TestValidateDatabaseEmptyTables(t *testing.T) {
	db := &Database{
		Name:    "app",
		Dialect: new(DialectMySQL),
		Tables:  []*Table{},
	}

	err := ValidateDatabase(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "schema is empty, declare some tables first")
}
