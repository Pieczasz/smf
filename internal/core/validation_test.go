package core

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidationErrorError(t *testing.T) {
	t.Run("error with field", func(t *testing.T) {
		err := &ValidationError{
			Entity:  "column",
			Name:    "email",
			Field:   "TypeRaw",
			Message: "column type is empty",
		}
		expected := `validation error in column "email" field "TypeRaw": column type is empty`
		assert.Equal(t, expected, err.Error())
	})

	t.Run("error without field", func(t *testing.T) {
		err := &ValidationError{
			Entity:  "table",
			Name:    "users",
			Message: "table has no columns",
		}
		expected := `validation error in table "users": table has no columns`
		assert.Equal(t, expected, err.Error())
	})

	t.Run("error with empty name", func(t *testing.T) {
		err := &ValidationError{
			Entity:  "database",
			Name:    "",
			Message: "database is nil",
		}
		expected := `validation error in database "": database is nil`
		assert.Equal(t, expected, err.Error())
	})
}

func TestDatabaseValidate(t *testing.T) {
	for _, tc := range databaseValidateCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			err := tc.db.Validate()
			if tc.wantErrContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrContains)
				return
			}
			assert.NoError(t, err)
		})
	}
}

var databaseValidateCases = []struct {
	name            string
	db              *Database
	wantErrContains string
}{
	{
		name:            "nil database",
		db:              nil,
		wantErrContains: "database is nil",
	},
	{
		name: "valid database",
		db: &Database{
			Name: "testdb",
			Tables: []*Table{
				{
					Name: "users",
					Columns: []*Column{
						{Name: "id", TypeRaw: "INT", PrimaryKey: true},
					},
				},
			},
		},
	},
	{
		name: "nil table in database",
		db: &Database{
			Name: "testdb",
			Tables: []*Table{
				{
					Name: "users",
					Columns: []*Column{
						{Name: "id", TypeRaw: "INT", PrimaryKey: true},
					},
				},
				nil,
			},
		},
		wantErrContains: "table at index 1 is nil",
	},
	{
		name: "duplicate table names",
		db: &Database{
			Name: "testdb",
			Tables: []*Table{
				{
					Name: "users",
					Columns: []*Column{
						{Name: "id", TypeRaw: "INT", PrimaryKey: true},
					},
				},
				{
					Name: "Users",
					Columns: []*Column{
						{Name: "id", TypeRaw: "INT", PrimaryKey: true},
					},
				},
			},
		},
		wantErrContains: "duplicate table name",
	},
	{
		name: "invalid table in database",
		db: &Database{
			Name: "testdb",
			Tables: []*Table{
				{
					Name:    "users",
					Columns: []*Column{},
				},
			},
		},
		wantErrContains: "table has no columns",
	},
	{
		name: "empty database with no tables",
		db: &Database{
			Name:   "testdb",
			Tables: []*Table{},
		},
	},
}

func TestTableValidate(t *testing.T) {
	for _, tc := range tableValidateCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			err := tc.table.Validate()
			if tc.wantErrContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrContains)
				return
			}
			assert.NoError(t, err)
		})
	}
}

var tableValidateCases = []struct {
	name            string
	table           *Table
	wantErrContains string
}{
	{name: "nil table", table: nil, wantErrContains: "table is nil"},
	{
		name: "empty table name",
		table: &Table{
			Name: "",
			Columns: []*Column{
				{Name: "id", TypeRaw: "INT"},
			},
		},
		wantErrContains: "table name is empty",
	},
	{
		name: "whitespace only table name",
		table: &Table{
			Name: "   ",
			Columns: []*Column{
				{Name: "id", TypeRaw: "INT"},
			},
		},
		wantErrContains: "table name is empty",
	},
	{
		name: "table with no columns",
		table: &Table{
			Name:    "users",
			Columns: []*Column{},
		},
		wantErrContains: "table has no columns",
	},
	{
		name: "nil column in table",
		table: &Table{
			Name: "users",
			Columns: []*Column{
				{Name: "id", TypeRaw: "INT"},
				nil,
			},
		},
		wantErrContains: "column at index 1 is nil",
	},
	{
		name: "invalid column in table",
		table: &Table{
			Name: "users",
			Columns: []*Column{
				{Name: "id", TypeRaw: "INT"},
				{Name: "", TypeRaw: "VARCHAR(255)"},
			},
		},
		wantErrContains: "column name is empty",
	},
	{
		name: "duplicate column names",
		table: &Table{
			Name: "users",
			Columns: []*Column{
				{Name: "id", TypeRaw: "INT"},
				{Name: "ID", TypeRaw: "INT"},
			},
		},
		wantErrContains: "duplicate column name",
	},
	{
		name: "nil constraint in table",
		table: &Table{
			Name: "users",
			Columns: []*Column{
				{Name: "id", TypeRaw: "INT"},
			},
			Constraints: []*Constraint{
				{Name: "pk_users", Type: ConstraintPrimaryKey, Columns: []string{"id"}},
				nil,
			},
		},
		wantErrContains: "constraint at index 1 is nil",
	},
	{
		name: "invalid constraint in table",
		table: &Table{
			Name: "users",
			Columns: []*Column{
				{Name: "id", TypeRaw: "INT"},
			},
			Constraints: []*Constraint{
				{Name: "pk_users", Type: ConstraintPrimaryKey, Columns: []string{}},
			},
		},
		wantErrContains: "constraint has no columns",
	},
	{
		name: "duplicate constraint names",
		table: &Table{
			Name: "users",
			Columns: []*Column{
				{Name: "id", TypeRaw: "INT"},
				{Name: "email", TypeRaw: "VARCHAR(255)"},
			},
			Constraints: []*Constraint{
				{Name: "pk_users", Type: ConstraintPrimaryKey, Columns: []string{"id"}},
				{Name: "PK_Users", Type: ConstraintUnique, Columns: []string{"email"}},
			},
		},
		wantErrContains: "duplicate constraint name",
	},
	{
		name: "empty constraint names are allowed",
		table: &Table{
			Name: "users",
			Columns: []*Column{
				{Name: "id", TypeRaw: "INT"},
				{Name: "email", TypeRaw: "VARCHAR(255)"},
			},
			Constraints: []*Constraint{
				{Name: "", Type: ConstraintPrimaryKey, Columns: []string{"id"}},
				{Name: "", Type: ConstraintUnique, Columns: []string{"email"}},
			},
		},
	},
	{
		name: "nil index in table",
		table: &Table{
			Name: "users",
			Columns: []*Column{
				{Name: "id", TypeRaw: "INT"},
			},
			Indexes: []*Index{
				{Name: "idx_id", Columns: []IndexColumn{{Name: "id"}}},
				nil,
			},
		},
		wantErrContains: "index at index 1 is nil",
	},
	{
		name: "invalid index in table",
		table: &Table{
			Name: "users",
			Columns: []*Column{
				{Name: "id", TypeRaw: "INT"},
			},
			Indexes: []*Index{
				{Name: "idx_id", Columns: []IndexColumn{}},
			},
		},
		wantErrContains: "index has no columns",
	},
	{
		name: "duplicate index names",
		table: &Table{
			Name: "users",
			Columns: []*Column{
				{Name: "id", TypeRaw: "INT"},
				{Name: "email", TypeRaw: "VARCHAR(255)"},
			},
			Indexes: []*Index{
				{Name: "idx_email", Columns: []IndexColumn{{Name: "email"}}},
				{Name: "IDX_Email", Columns: []IndexColumn{{Name: "id"}}},
			},
		},
		wantErrContains: "duplicate index name",
	},
	{
		name: "empty index names are allowed",
		table: &Table{
			Name: "users",
			Columns: []*Column{
				{Name: "id", TypeRaw: "INT"},
				{Name: "email", TypeRaw: "VARCHAR(255)"},
			},
			Indexes: []*Index{
				{Name: "", Columns: []IndexColumn{{Name: "id"}}},
				{Name: "", Columns: []IndexColumn{{Name: "email"}}},
			},
		},
	},
	{
		name: "valid table with all components",
		table: &Table{
			Name: "users",
			Columns: []*Column{
				{Name: "id", TypeRaw: "INT"},
				{Name: "email", TypeRaw: "VARCHAR(255)"},
			},
			Constraints: []*Constraint{
				{Name: "pk_users", Type: ConstraintPrimaryKey, Columns: []string{"id"}},
			},
			Indexes: []*Index{
				{Name: "idx_email", Columns: []IndexColumn{{Name: "email"}}},
			},
		},
	},
}

func TestColumnLevelPrimaryKeyOnly(t *testing.T) {
	table := &Table{
		Name: "users",
		Columns: []*Column{
			{Name: "id", TypeRaw: "INT", PrimaryKey: true},
			{Name: "email", TypeRaw: "VARCHAR(255)"},
		},
	}
	err := table.Validate()
	assert.NoError(t, err)
}

func TestConstraintLevelPrimaryKeyOnly(t *testing.T) {
	table := &Table{
		Name: "users",
		Columns: []*Column{
			{Name: "id", TypeRaw: "INT"},
			{Name: "email", TypeRaw: "VARCHAR(255)"},
		},
		Constraints: []*Constraint{
			{Type: ConstraintPrimaryKey, Columns: []string{"id"}},
		},
	}
	err := table.Validate()
	assert.NoError(t, err)
}

func TestCompositePrimaryKeyInConstraints(t *testing.T) {
	table := &Table{
		Name: "user_roles",
		Columns: []*Column{
			{Name: "user_id", TypeRaw: "INT"},
			{Name: "role_id", TypeRaw: "INT"},
		},
		Constraints: []*Constraint{
			{Type: ConstraintPrimaryKey, Columns: []string{"user_id", "role_id"}},
		},
	}
	err := table.Validate()
	assert.NoError(t, err)
}

func TestColumnPKPlusSynthesizedConstraint(t *testing.T) {
	// After synthesis, a table has both column-level PrimaryKey=true
	// and a single PK constraint — this is the normal state and must
	// pass core validation. The column-vs-constraint conflict check
	// lives in the parser (before synthesis), not in core.
	table := &Table{
		Name: "users",
		Columns: []*Column{
			{Name: "id", TypeRaw: "INT", PrimaryKey: true},
			{Name: "email", TypeRaw: "VARCHAR(255)"},
		},
		Constraints: []*Constraint{
			{Name: "pk_users", Type: ConstraintPrimaryKey, Columns: []string{"id"}},
		},
	}
	err := table.Validate()
	assert.NoError(t, err)
}

func TestMultiplePrimaryKeyConstraintsError(t *testing.T) {
	table := &Table{
		Name: "users",
		Columns: []*Column{
			{Name: "id", TypeRaw: "INT"},
			{Name: "email", TypeRaw: "VARCHAR(255)"},
		},
		Constraints: []*Constraint{
			{Type: ConstraintPrimaryKey, Columns: []string{"id"}},
			{Type: ConstraintPrimaryKey, Columns: []string{"email"}},
		},
	}
	err := table.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "multiple PRIMARY KEY constraints")
}

func TestNoPrimaryKeyIsValid(t *testing.T) {
	table := &Table{
		Name: "logs",
		Columns: []*Column{
			{Name: "message", TypeRaw: "TEXT"},
		},
	}
	err := table.Validate()
	assert.NoError(t, err)
}

func TestColumnValidate(t *testing.T) {
	t.Run("nil column", func(t *testing.T) {
		var col *Column
		err := col.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "column is nil")
	})

	t.Run("empty column name", func(t *testing.T) {
		col := &Column{Name: "", TypeRaw: "INT"}
		err := col.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "column name is empty")
	})

	t.Run("whitespace only column name", func(t *testing.T) {
		col := &Column{Name: "   ", TypeRaw: "INT"}
		err := col.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "column name is empty")
	})

	t.Run("empty column type", func(t *testing.T) {
		col := &Column{Name: "id", TypeRaw: ""}
		err := col.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "column type is empty")
	})

	t.Run("whitespace only column type", func(t *testing.T) {
		col := &Column{Name: "id", TypeRaw: "   "}
		err := col.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "column type is empty")
	})

	t.Run("generated column without expression", func(t *testing.T) {
		col := &Column{
			Name:                 "full_name",
			TypeRaw:              "VARCHAR(255)",
			IsGenerated:          true,
			GenerationExpression: "",
		}
		err := col.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "generated column must have an expression")
	})

	t.Run("generated column with whitespace only expression", func(t *testing.T) {
		col := &Column{
			Name:                 "full_name",
			TypeRaw:              "VARCHAR(255)",
			IsGenerated:          true,
			GenerationExpression: "   ",
		}
		err := col.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "generated column must have an expression")
	})

	t.Run("valid generated column", func(t *testing.T) {
		col := &Column{
			Name:                 "full_name",
			TypeRaw:              "VARCHAR(255)",
			IsGenerated:          true,
			GenerationExpression: "CONCAT(first_name, ' ', last_name)",
			GenerationStorage:    GenerationVirtual,
		}
		err := col.Validate()
		assert.NoError(t, err)
	})

	t.Run("valid regular column", func(t *testing.T) {
		col := &Column{
			Name:    "email",
			TypeRaw: "VARCHAR(255)",
		}
		err := col.Validate()
		assert.NoError(t, err)
	})
}

func TestColumnInlineReferencesValidation(t *testing.T) {
	t.Run("valid inline reference", func(t *testing.T) {
		col := &Column{
			Name:       "tenant_id",
			TypeRaw:    "BIGINT",
			References: "tenants.id",
		}
		err := col.Validate()
		assert.NoError(t, err)
	})

	t.Run("invalid reference format no dot", func(t *testing.T) {
		col := &Column{
			Name:       "tenant_id",
			TypeRaw:    "BIGINT",
			References: "tenants",
		}
		err := col.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid reference format")
		assert.Contains(t, err.Error(), "expected \"table.column\"")
	})

	t.Run("invalid reference format dot at end", func(t *testing.T) {
		col := &Column{
			Name:       "tenant_id",
			TypeRaw:    "BIGINT",
			References: "tenants.",
		}
		err := col.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid reference format")
	})

	t.Run("invalid reference format dot at start", func(t *testing.T) {
		col := &Column{
			Name:       "tenant_id",
			TypeRaw:    "BIGINT",
			References: ".id",
		}
		err := col.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid reference format")
	})

	t.Run("schema-qualified reference is valid", func(t *testing.T) {
		col := &Column{
			Name:       "user_id",
			TypeRaw:    "BIGINT",
			References: "public.users.id",
		}
		err := col.Validate()
		assert.NoError(t, err)
	})

	t.Run("empty references is valid (no FK)", func(t *testing.T) {
		col := &Column{
			Name:    "email",
			TypeRaw: "VARCHAR(255)",
		}
		err := col.Validate()
		assert.NoError(t, err)
	})
}

func TestColumnEnumValuesValidation(t *testing.T) {
	t.Run("enum with values array is valid", func(t *testing.T) {
		col := &Column{
			Name:       "status",
			TypeRaw:    "enum('active','inactive')",
			Type:       DataTypeEnum,
			EnumValues: []string{"active", "inactive"},
		}
		err := col.Validate()
		assert.NoError(t, err)
	})

	t.Run("enum without values but with legacy inline type is valid", func(t *testing.T) {
		col := &Column{
			Name:    "status",
			TypeRaw: "enum('active','inactive')",
			Type:    DataTypeEnum,
		}
		err := col.Validate()
		assert.NoError(t, err)
	})

	t.Run("enum without values and without inline type fails", func(t *testing.T) {
		col := &Column{
			Name:    "status",
			TypeRaw: "enum",
			Type:    DataTypeEnum,
		}
		err := col.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "enum column must have values")
	})

	t.Run("non-enum type does not require values", func(t *testing.T) {
		col := &Column{
			Name:    "name",
			TypeRaw: "VARCHAR(255)",
			Type:    DataTypeString,
		}
		err := col.Validate()
		assert.NoError(t, err)
	})
}

func TestEnabledTimestampsIsValid(t *testing.T) {
	table := &Table{
		Name: "events",
		Columns: []*Column{
			{Name: "id", TypeRaw: "INT"},
		},
		Timestamps: &TimestampsConfig{Enabled: true},
	}
	err := table.Validate()
	assert.NoError(t, err)
}

func TestDisabledTimestampsIsValid(t *testing.T) {
	table := &Table{
		Name: "events",
		Columns: []*Column{
			{Name: "id", TypeRaw: "INT"},
		},
		Timestamps: &TimestampsConfig{Enabled: false},
	}
	err := table.Validate()
	assert.NoError(t, err)
}

func TestCustomTimestampColumnNamesAreValid(t *testing.T) {
	table := &Table{
		Name: "events",
		Columns: []*Column{
			{Name: "id", TypeRaw: "INT"},
		},
		Timestamps: &TimestampsConfig{
			Enabled:       true,
			CreatedColumn: "inserted_at",
			UpdatedColumn: "modified_at",
		},
	}
	err := table.Validate()
	assert.NoError(t, err)
}

func TestSameTimestampColumnNamesIsInvalid(t *testing.T) {
	table := &Table{
		Name: "events",
		Columns: []*Column{
			{Name: "id", TypeRaw: "INT"},
		},
		Timestamps: &TimestampsConfig{
			Enabled:       true,
			CreatedColumn: "ts",
			UpdatedColumn: "ts",
		},
	}
	err := table.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "created and updated column names must differ")
}

func TestCaseInsensitiveSameTimestampNamesIsInvalid(t *testing.T) {
	table := &Table{
		Name: "events",
		Columns: []*Column{
			{Name: "id", TypeRaw: "INT"},
		},
		Timestamps: &TimestampsConfig{
			Enabled:       true,
			CreatedColumn: "TS",
			UpdatedColumn: "ts",
		},
	}
	err := table.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "created and updated column names must differ")
}

func TestNilTimestampsIsValid(t *testing.T) {
	table := &Table{
		Name: "events",
		Columns: []*Column{
			{Name: "id", TypeRaw: "INT"},
		},
	}
	err := table.Validate()
	assert.NoError(t, err)
}

func TestTableNameExceedsMaxLength(t *testing.T) {
	db := &Database{
		Name: "testdb",
		Validation: &ValidationRules{
			MaxTableNameLength: 5,
		},
		Tables: []*Table{
			{
				Name: "very_long_table_name",
				Columns: []*Column{
					{Name: "id", TypeRaw: "INT"},
				},
			},
		},
	}
	err := db.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds maximum length")
}

func TestTableNameWithinMaxLength(t *testing.T) {
	db := &Database{
		Name: "testdb",
		Validation: &ValidationRules{
			MaxTableNameLength: 64,
		},
		Tables: []*Table{
			{
				Name: "users",
				Columns: []*Column{
					{Name: "id", TypeRaw: "INT"},
				},
			},
		},
	}
	err := db.Validate()
	assert.NoError(t, err)
}

func TestColumnNameExceedsMaxLength(t *testing.T) {
	db := &Database{
		Name: "testdb",
		Validation: &ValidationRules{
			MaxColumnNameLength: 3,
		},
		Tables: []*Table{
			{
				Name: "t",
				Columns: []*Column{
					{Name: "very_long_column_name", TypeRaw: "INT"},
				},
			},
		},
	}
	err := db.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds maximum length")
}

func TestColumnNameWithinMaxLength(t *testing.T) {
	db := &Database{
		Name: "testdb",
		Validation: &ValidationRules{
			MaxColumnNameLength: 64,
		},
		Tables: []*Table{
			{
				Name: "t",
				Columns: []*Column{
					{Name: "id", TypeRaw: "INT"},
				},
			},
		},
	}
	err := db.Validate()
	assert.NoError(t, err)
}

func TestZeroMaxLengthMeansNoLimit(t *testing.T) {
	db := &Database{
		Name: "testdb",
		Validation: &ValidationRules{
			MaxTableNameLength:  0,
			MaxColumnNameLength: 0,
		},
		Tables: []*Table{
			{
				Name: "a_very_long_table_name_that_would_normally_fail",
				Columns: []*Column{
					{Name: "a_very_long_column_name_that_would_normally_fail", TypeRaw: "INT"},
				},
			},
		},
	}
	err := db.Validate()
	assert.NoError(t, err)
}

func TestTableNameMatchesPattern(t *testing.T) {
	db := &Database{
		Name: "testdb",
		Validation: &ValidationRules{
			AllowedNamePattern: `^[a-z][a-z0-9_]*$`,
		},
		Tables: []*Table{
			{
				Name: "users",
				Columns: []*Column{
					{Name: "id", TypeRaw: "INT"},
				},
			},
		},
	}
	err := db.Validate()
	assert.NoError(t, err)
}

func TestTableNameDoesNotMatchPattern(t *testing.T) {
	db := &Database{
		Name: "testdb",
		Validation: &ValidationRules{
			AllowedNamePattern: `^[a-z][a-z0-9_]*$`,
		},
		Tables: []*Table{
			{
				Name: "Users",
				Columns: []*Column{
					{Name: "id", TypeRaw: "INT"},
				},
			},
		},
	}
	err := db.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not match allowed pattern")
}

func TestColumnNameDoesNotMatchPattern(t *testing.T) {
	db := &Database{
		Name: "testdb",
		Validation: &ValidationRules{
			AllowedNamePattern: `^[a-z][a-z0-9_]*$`,
		},
		Tables: []*Table{
			{
				Name: "users",
				Columns: []*Column{
					{Name: "ID", TypeRaw: "INT"},
				},
			},
		},
	}
	err := db.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not match allowed pattern")
}

func TestInvalidRegexInPattern(t *testing.T) {
	db := &Database{
		Name: "testdb",
		Validation: &ValidationRules{
			AllowedNamePattern: `[invalid`,
		},
		Tables: []*Table{
			{
				Name: "users",
				Columns: []*Column{
					{Name: "id", TypeRaw: "INT"},
				},
			},
		},
	}
	err := db.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid regex")
}

func TestEmptyPatternMeansNoCheck(t *testing.T) {
	db := &Database{
		Name: "testdb",
		Validation: &ValidationRules{
			AllowedNamePattern: "",
		},
		Tables: []*Table{
			{
				Name: "ANY Name Allowed!",
				Columns: []*Column{
					{Name: "Whatever 123", TypeRaw: "INT"},
				},
			},
		},
	}
	err := db.Validate()
	assert.NoError(t, err)
}

func TestNilValidationMeansNoChecks(t *testing.T) {
	db := &Database{
		Name: "testdb",
		Tables: []*Table{
			{
				Name: "ANY Name",
				Columns: []*Column{
					{Name: "Whatever", TypeRaw: "INT"},
				},
			},
		},
	}
	err := db.Validate()
	assert.NoError(t, err)
}

func TestConstraintValidate(t *testing.T) {
	for _, tc := range constraintValidateCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			err := tc.constraint.Validate()
			if tc.wantErrContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrContains)
				return
			}
			assert.NoError(t, err)
		})
	}
}

var constraintValidateCases = []struct {
	name            string
	constraint      *Constraint
	wantErrContains string
}{
	{name: "nil constraint", constraint: nil, wantErrContains: "constraint is nil"},
	{
		name: "primary key without columns",
		constraint: &Constraint{
			Name:    "pk_users",
			Type:    ConstraintPrimaryKey,
			Columns: []string{},
		},
		wantErrContains: "constraint has no columns",
	},
	{
		name: "unique constraint without columns",
		constraint: &Constraint{
			Name:    "uq_email",
			Type:    ConstraintUnique,
			Columns: []string{},
		},
		wantErrContains: "constraint has no columns",
	},
	{
		name: "foreign key without referenced table",
		constraint: &Constraint{
			Name:              "fk_order_user",
			Type:              ConstraintForeignKey,
			Columns:           []string{"user_id"},
			ReferencedTable:   "",
			ReferencedColumns: []string{"id"},
		},
		wantErrContains: "foreign key must reference a table",
	},
	{
		name: "foreign key with whitespace only referenced table",
		constraint: &Constraint{
			Name:              "fk_order_user",
			Type:              ConstraintForeignKey,
			Columns:           []string{"user_id"},
			ReferencedTable:   "   ",
			ReferencedColumns: []string{"id"},
		},
		wantErrContains: "foreign key must reference a table",
	},
	{
		name: "foreign key without referenced columns",
		constraint: &Constraint{
			Name:              "fk_order_user",
			Type:              ConstraintForeignKey,
			Columns:           []string{"user_id"},
			ReferencedTable:   "users",
			ReferencedColumns: []string{},
		},
		wantErrContains: "foreign key must reference columns",
	},
	{
		name: "foreign key column count mismatch",
		constraint: &Constraint{
			Name:              "fk_order_user",
			Type:              ConstraintForeignKey,
			Columns:           []string{"user_id", "org_id"},
			ReferencedTable:   "users",
			ReferencedColumns: []string{"id"},
		},
		wantErrContains: "foreign key column count mismatch",
	},
	{
		name: "check constraint without expression",
		constraint: &Constraint{
			Name:            "chk_age",
			Type:            ConstraintCheck,
			CheckExpression: "",
		},
		wantErrContains: "check constraint must have an expression",
	},
	{
		name: "check constraint with whitespace only expression",
		constraint: &Constraint{
			Name:            "chk_age",
			Type:            ConstraintCheck,
			CheckExpression: "   ",
		},
		wantErrContains: "check constraint must have an expression",
	},
	{
		name: "valid primary key constraint",
		constraint: &Constraint{
			Name:    "pk_users",
			Type:    ConstraintPrimaryKey,
			Columns: []string{"id"},
		},
	},
	{
		name: "valid unique constraint",
		constraint: &Constraint{
			Name:    "uq_email",
			Type:    ConstraintUnique,
			Columns: []string{"email"},
		},
	},
	{
		name: "valid foreign key constraint",
		constraint: &Constraint{
			Name:              "fk_order_user",
			Type:              ConstraintForeignKey,
			Columns:           []string{"user_id"},
			ReferencedTable:   "users",
			ReferencedColumns: []string{"id"},
			OnDelete:          RefActionCascade,
			OnUpdate:          RefActionRestrict,
		},
	},
	{
		name: "valid check constraint",
		constraint: &Constraint{
			Name:            "chk_age",
			Type:            ConstraintCheck,
			CheckExpression: "age >= 0 AND age <= 150",
			Enforced:        true,
		},
	},
	{
		name: "check constraint can have empty columns",
		constraint: &Constraint{
			Name:            "chk_age",
			Type:            ConstraintCheck,
			Columns:         []string{},
			CheckExpression: "age >= 0",
		},
	},
}

func TestIndexValidate(t *testing.T) {
	for _, tc := range indexValidateCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			err := tc.index.Validate()
			if tc.wantErrContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrContains)
				return
			}
			assert.NoError(t, err)
		})
	}
}

var indexValidateCases = []struct {
	name            string
	index           *Index
	wantErrContains string
}{
	{name: "nil index", index: nil, wantErrContains: "index is nil"},
	{
		name: "index without columns",
		index: &Index{
			Name:    "idx_email",
			Columns: []IndexColumn{},
		},
		wantErrContains: "index has no columns",
	},
	{
		name: "index with empty column name",
		index: &Index{
			Name: "idx_composite",
			Columns: []IndexColumn{
				{Name: "email"},
				{Name: ""},
			},
		},
		wantErrContains: "index column at position 1 has empty name",
	},
	{
		name: "index with whitespace only column name",
		index: &Index{
			Name: "idx_composite",
			Columns: []IndexColumn{
				{Name: "   "},
			},
		},
		wantErrContains: "index column at position 0 has empty name",
	},
	{
		name: "valid index with single column",
		index: &Index{
			Name: "idx_email",
			Columns: []IndexColumn{
				{Name: "email"},
			},
		},
	},
	{
		name: "valid index with multiple columns",
		index: &Index{
			Name: "idx_composite",
			Columns: []IndexColumn{
				{Name: "first_name", Order: SortAsc},
				{Name: "last_name", Order: SortDesc},
			},
			Unique: true,
			Type:   IndexTypeBTree,
		},
	},
	{
		name: "valid index with length",
		index: &Index{
			Name: "idx_content",
			Columns: []IndexColumn{
				{Name: "content", Length: 100},
			},
		},
	},
	{
		name: "valid index without name",
		index: &Index{
			Columns: []IndexColumn{
				{Name: "email"},
			},
		},
	},
}

func TestValidationRulesCombined(t *testing.T) {
	t.Run("all rules pass", func(t *testing.T) {
		db := &Database{
			Name: "testdb",
			Validation: &ValidationRules{
				MaxTableNameLength:  64,
				MaxColumnNameLength: 64,
				AllowedNamePattern:  `^[a-z_][a-z0-9_]*$`,
			},
			Tables: []*Table{
				{
					Name: "users",
					Columns: []*Column{
						{Name: "id", TypeRaw: "INT", PrimaryKey: true},
						{Name: "email", TypeRaw: "VARCHAR(255)"},
					},
				},
			},
		}
		err := db.Validate()
		assert.NoError(t, err)
	})

	t.Run("table name fails pattern while column name fails length", func(t *testing.T) {
		db := &Database{
			Name: "testdb",
			Validation: &ValidationRules{
				MaxTableNameLength:  64,
				MaxColumnNameLength: 2,
				AllowedNamePattern:  `^[a-z_][a-z0-9_]*$`,
			},
			Tables: []*Table{
				{
					Name: "Users", // fails pattern (uppercase U)
					Columns: []*Column{
						{Name: "id", TypeRaw: "INT"},
					},
				},
			},
		}
		err := db.Validate()
		require.Error(t, err)
		// Table-level pattern check fires before column-level
		assert.Contains(t, err.Error(), "does not match allowed pattern")
	})
}
