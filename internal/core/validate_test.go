package core

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateDatabaseSuccessAndSynthesis(t *testing.T) {
	d := DialectMySQL
	db := &Database{
		Name:    "app",
		Dialect: &d,
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
	d := DialectMySQL
	db := &Database{
		Name:    "app",
		Dialect: &d,
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
	d := DialectMySQL
	db := &Database{
		Name:    "app",
		Dialect: &d,
		Tables: []*Table{
			{Name: "users", Columns: []*Column{{Name: "id"}}},
			{Name: "Users", Columns: []*Column{{Name: "id"}}},
		},
	}

	err := ValidateDatabase(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate table name")
}

func TestValidateDatabaseTableNameValidation(t *testing.T) {
	d := DialectMySQL

	t.Run("empty table name", func(t *testing.T) {
		db := &Database{
			Name:    "app",
			Dialect: &d,
			Tables: []*Table{
				{Name: "   ", Columns: []*Column{{Name: "id"}}},
			},
		}

		err := ValidateDatabase(db)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "table")
		assert.Contains(t, err.Error(), "name is empty")
	})

	t.Run("max table name length", func(t *testing.T) {
		db := &Database{
			Name:    "app",
			Dialect: &d,
			Validation: &ValidationRules{
				MaxTableNameLength: 3,
			},
			Tables: []*Table{
				{Name: "users", Columns: []*Column{{Name: "id"}}},
			},
		}

		err := ValidateDatabase(db)
		require.Error(t, err)
		assert.Contains(t, err.Error(), `table "users" exceeds maximum length 3`)
	})

	t.Run("allowed name pattern for table", func(t *testing.T) {
		db := &Database{
			Name:    "app",
			Dialect: &d,
			Validation: &ValidationRules{
				AllowedNamePattern: `^[a-z_]+$`,
			},
			Tables: []*Table{
				{Name: "Users", Columns: []*Column{{Name: "id"}}},
			},
		}

		err := ValidateDatabase(db)
		require.Error(t, err)
		assert.Contains(t, err.Error(), `does not match allowed pattern`)
	})
}

func TestValidateDatabaseColumnsValidation(t *testing.T) {
	d := DialectMySQL

	t.Run("table has no columns", func(t *testing.T) {
		db := &Database{
			Name:    "app",
			Dialect: &d,
			Tables: []*Table{
				{Name: "users"},
			},
		}

		err := ValidateDatabase(db)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "table has no columns")
	})

	t.Run("duplicate column names case insensitive", func(t *testing.T) {
		db := &Database{
			Name:    "app",
			Dialect: &d,
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
	})

	t.Run("empty column name", func(t *testing.T) {
		db := &Database{
			Name:    "app",
			Dialect: &d,
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
	})

	t.Run("max column name length", func(t *testing.T) {
		db := &Database{
			Name:    "app",
			Dialect: &d,
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
	})

	t.Run("allowed name pattern for column", func(t *testing.T) {
		db := &Database{
			Name:    "app",
			Dialect: &d,
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
		assert.Contains(t, err.Error(), `does not match allowed pattern`)
	})

	t.Run("invalid references format", func(t *testing.T) {
		db := &Database{
			Name:    "app",
			Dialect: &d,
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
	})
}

func TestValidateDatabasePKConflictValidation(t *testing.T) {
	d := DialectMySQL

	t.Run("multiple primary key constraints", func(t *testing.T) {
		db := &Database{
			Name:    "app",
			Dialect: &d,
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
	})

	t.Run("column pk and constraint pk both present", func(t *testing.T) {
		db := &Database{
			Name:    "app",
			Dialect: &d,
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
	})
}

func TestValidateDatabaseConstraintValidation(t *testing.T) {
	d := DialectMySQL

	t.Run("duplicate constraint names case insensitive", func(t *testing.T) {
		db := &Database{
			Name:    "app",
			Dialect: &d,
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
	})

	t.Run("constraint with no columns", func(t *testing.T) {
		db := &Database{
			Name:    "app",
			Dialect: &d,
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
	})

	t.Run("constraint references nonexistent column", func(t *testing.T) {
		db := &Database{
			Name:    "app",
			Dialect: &d,
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
	})

	t.Run("foreign key missing referenced table", func(t *testing.T) {
		db := &Database{
			Name:    "app",
			Dialect: &d,
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
	})

	t.Run("foreign key missing referenced columns", func(t *testing.T) {
		db := &Database{
			Name:    "app",
			Dialect: &d,
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
	})

	t.Run("check constraint may have no columns", func(t *testing.T) {
		db := &Database{
			Name:    "app",
			Dialect: &d,
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
	})
}

func TestValidateDatabaseIndexValidation(t *testing.T) {
	d := DialectMySQL

	t.Run("duplicate index names case insensitive", func(t *testing.T) {
		db := &Database{
			Name:    "app",
			Dialect: &d,
			Tables: []*Table{
				{
					Name:    "users",
					Columns: []*Column{{Name: "email"}},
					Indexes: []*Index{
						{Name: "idx_email", Columns: []ColumnIndex{{Name: "email"}}},
						{Name: "IDX_EMAIL", Columns: []ColumnIndex{{Name: "email"}}},
					},
				},
			},
		}

		err := ValidateDatabase(db)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "duplicate index name")
	})

	t.Run("index has no columns", func(t *testing.T) {
		db := &Database{
			Name:    "app",
			Dialect: &d,
			Tables: []*Table{
				{
					Name:    "users",
					Columns: []*Column{{Name: "email"}},
					Indexes: []*Index{
						{Name: "idx_email"},
					},
				},
			},
		}

		err := ValidateDatabase(db)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "index idx_email has no columns")
	})

	t.Run("unnamed index has no columns", func(t *testing.T) {
		db := &Database{
			Name:    "app",
			Dialect: &d,
			Tables: []*Table{
				{
					Name:    "users",
					Columns: []*Column{{Name: "email"}},
					Indexes: []*Index{
						{},
					},
				},
			},
		}

		err := ValidateDatabase(db)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "index (unnamed) has no columns")
	})

	t.Run("index references nonexistent column", func(t *testing.T) {
		db := &Database{
			Name:    "app",
			Dialect: &d,
			Tables: []*Table{
				{
					Name:    "users",
					Columns: []*Column{{Name: "email"}},
					Indexes: []*Index{
						{Name: "idx_missing", Columns: []ColumnIndex{{Name: "missing"}}},
					},
				},
			},
		}

		err := ValidateDatabase(db)
		require.Error(t, err)
		assert.Contains(t, err.Error(), `index "idx_missing" references nonexistent column "missing"`)
	})
}

func TestValidateDatabaseTimestampsValidation(t *testing.T) {
	d := DialectMySQL

	t.Run("disabled timestamps", func(t *testing.T) {
		db := &Database{
			Name:    "app",
			Dialect: &d,
			Tables: []*Table{
				{
					Name:       "users",
					Columns:    []*Column{{Name: "id"}},
					Timestamps: &TimestampsConfig{Enabled: false},
				},
			},
		}

		err := ValidateDatabase(db)
		require.NoError(t, err)
	})

	t.Run("default created and updated names are distinct", func(t *testing.T) {
		db := &Database{
			Name:    "app",
			Dialect: &d,
			Tables: []*Table{
				{
					Name:       "users",
					Columns:    []*Column{{Name: "id"}},
					Timestamps: &TimestampsConfig{Enabled: true},
				},
			},
		}

		err := ValidateDatabase(db)
		require.NoError(t, err)
	})

	t.Run("same timestamps names case insensitive", func(t *testing.T) {
		db := &Database{
			Name:    "app",
			Dialect: &d,
			Tables: []*Table{
				{
					Name:    "users",
					Columns: []*Column{{Name: "id"}},
					Timestamps: &TimestampsConfig{
						Enabled:       true,
						CreatedColumn: "created_at",
						UpdatedColumn: "CREATED_AT",
					},
				},
			},
		}

		err := ValidateDatabase(db)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "resolve to the same name")
	})
}

func TestValidateDatabaseErrorPrefixIncludesTableName(t *testing.T) {
	d := DialectMySQL
	db := &Database{
		Name:    "app",
		Dialect: &d,
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
	assert.True(t, strings.Contains(err.Error(), `table "users":`))
}

func TestSynthesizeConstraintsPK(t *testing.T) {
	t.Run("synthesizes PK from column flags", func(t *testing.T) {
		table := &Table{
			Name: "users",
			Columns: []*Column{
				{Name: "id", PrimaryKey: true},
				{Name: "tenant_id", PrimaryKey: true},
			},
		}

		synthesizeConstraints(table)

		pk := table.PrimaryKey()
		require.NotNil(t, pk)
		assert.Equal(t, ConstraintPrimaryKey, pk.Type)
		assert.Equal(t, []string{"id", "tenant_id"}, pk.Columns)
	})

	t.Run("does not synthesize PK when table already has PK", func(t *testing.T) {
		table := &Table{
			Name: "users",
			Columns: []*Column{
				{Name: "id", PrimaryKey: true},
			},
			Constraints: []*Constraint{
				{Name: "pk_users", Type: ConstraintPrimaryKey, Columns: []string{"id"}},
			},
		}

		synthesizeConstraints(table)

		var pkCount int
		for _, c := range table.Constraints {
			if c.Type == ConstraintPrimaryKey {
				pkCount++
			}
		}
		assert.Equal(t, 1, pkCount)
	})
}

func TestSynthesizeConstraintsUniqueAndCheck(t *testing.T) {
	table := &Table{
		Name: "users",
		Columns: []*Column{
			{Name: "email", Unique: true},
			{Name: "age", Check: "age >= 0"},
		},
	}

	synthesizeConstraints(table)

	var uniqueFound, checkFound bool
	for _, c := range table.Constraints {
		if c.Type == ConstraintUnique && len(c.Columns) == 1 && c.Columns[0] == "email" {
			uniqueFound = true
		}
		if c.Type == ConstraintCheck && c.CheckExpression == "age >= 0" {
			checkFound = true
			assert.True(t, c.Enforced)
		}
	}
	assert.True(t, uniqueFound)
	assert.True(t, checkFound)
}

func TestSynthesizeConstraintsFK(t *testing.T) {
	t.Run("synthesizes FK from valid references", func(t *testing.T) {
		table := &Table{
			Name: "users",
			Columns: []*Column{
				{Name: "role_id", References: "roles.id", RefOnDelete: "CASCADE", RefOnUpdate: "SET NULL"},
			},
		}

		synthesizeConstraints(table)

		var fk *Constraint
		for _, c := range table.Constraints {
			if c.Type == ConstraintForeignKey {
				fk = c
				break
			}
		}
		require.NotNil(t, fk)
		assert.Equal(t, []string{"role_id"}, fk.Columns)
		assert.Equal(t, "roles", fk.ReferencedTable)
		assert.Equal(t, []string{"id"}, fk.ReferencedColumns)
		assert.Equal(t, ReferentialAction("CASCADE"), fk.OnDelete)
		assert.Equal(t, ReferentialAction("SET NULL"), fk.OnUpdate)
		assert.True(t, fk.Enforced)
	})

	t.Run("skips invalid references in synthesis", func(t *testing.T) {
		table := &Table{
			Name: "users",
			Columns: []*Column{
				{Name: "role_id", References: "invalid"},
			},
		}

		synthesizeConstraints(table)

		for _, c := range table.Constraints {
			assert.NotEqual(t, ConstraintForeignKey, c.Type)
		}
	})
}
