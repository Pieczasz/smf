package validate

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"smf/internal/schema"
)

func TestAutoIncrementOnNonInteger(t *testing.T) {
	db := &schema.Database{
		Name:    "app",
		Dialect: schema.DialectMySQL,
		Tables: []*schema.Table{
			{
				Name: "users",
				Columns: []*schema.Column{
					{Name: "id", Type: schema.DataTypeString, AutoIncrement: true},
				},
			},
		},
	}

	err := Database(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "auto_increment is only allowed on integer columns")
}

func TestAutoIncrementSQLiteOnNonPK(t *testing.T) {
	db := &schema.Database{
		Name:    "app",
		Dialect: schema.DialectSQLite,
		Tables: []*schema.Table{
			{
				Name: "users",
				Columns: []*schema.Column{
					{Name: "id", Type: schema.DataTypeInt, AutoIncrement: true},
				},
			},
		},
	}

	err := Database(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "SQLite AUTOINCREMENT is only allowed on PRIMARY KEY columns")
}

func TestAutoIncrementValid(t *testing.T) {
	db := &schema.Database{
		Name:    "app",
		Dialect: schema.DialectMySQL,
		Tables: []*schema.Table{
			{
				Name: "users",
				Columns: []*schema.Column{
					{Name: "id", Type: schema.DataTypeInt, AutoIncrement: true, PrimaryKey: true},
				},
			},
		},
	}

	err := Database(db)
	require.NoError(t, err)
}

func TestNullablePKColumnLevel(t *testing.T) {
	db := &schema.Database{
		Name:    "app",
		Dialect: schema.DialectMySQL,
		Tables: []*schema.Table{
			{
				Name: "users",
				Columns: []*schema.Column{
					{Name: "id", Type: schema.DataTypeInt, PrimaryKey: true, Nullable: true},
				},
			},
		},
	}

	err := Database(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "primary key columns cannot be nullable")
}

func TestNullablePKTableLevel(t *testing.T) {
	db := &schema.Database{
		Name:    "app",
		Dialect: schema.DialectMySQL,
		Tables: []*schema.Table{
			{
				Name: "users",
				Columns: []*schema.Column{
					{Name: "id", Type: schema.DataTypeInt, Nullable: true},
				},
				Constraints: []*schema.Constraint{
					{
						Type:    schema.ConstraintPrimaryKey,
						Columns: []string{"id"},
					},
				},
			},
		},
	}

	err := Database(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "primary key columns cannot be nullable")
}

func TestGeneratedColumnWithoutExpression(t *testing.T) {
	db := &schema.Database{
		Name:    "app",
		Dialect: schema.DialectMySQL,
		Tables: []*schema.Table{
			{
				Name: "users",
				Columns: []*schema.Column{
					{Name: "id", Type: schema.DataTypeInt, PrimaryKey: true},
					{Name: "full_name", Type: schema.DataTypeString, IsGenerated: true},
				},
			},
		},
	}

	err := Database(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "generated column must have an expression")
}

func TestIdentityOnNonAutoIncrement(t *testing.T) {
	db := &schema.Database{
		Name:    "app",
		Dialect: schema.DialectMySQL,
		Tables: []*schema.Table{
			{
				Name: "users",
				Columns: []*schema.Column{
					{Name: "id", Type: schema.DataTypeInt, PrimaryKey: true, IdentitySeed: 100},
				},
			},
		},
	}

	err := Database(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "identity_seed and identity_increment can only be set for auto_increment columns")
}

func TestTiDBAutoRandomOnNonPK(t *testing.T) {
	db := &schema.Database{
		Name:    "app",
		Dialect: schema.DialectTiDB,
		Tables: []*schema.Table{
			{
				Name: "users",
				Columns: []*schema.Column{
					{
						Name: "id",
						Type: schema.DataTypeInt,
						TiDB: &schema.TiDBColumnOptions{ShardBits: 5},
					},
				},
			},
		},
	}

	err := Database(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "TiDB AUTO_RANDOM can only be applied to BIGINT PRIMARY KEY columns")
}

func TestTiDBAutoRandomOnNonInteger(t *testing.T) {
	db := &schema.Database{
		Name:    "app",
		Dialect: schema.DialectTiDB,
		Tables: []*schema.Table{
			{
				Name: "users",
				Columns: []*schema.Column{
					{
						Name:       "id",
						Type:       schema.DataTypeString,
						PrimaryKey: true,
						TiDB:       &schema.TiDBColumnOptions{ShardBits: 5},
					},
				},
			},
		},
	}

	err := Database(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "TiDB AUTO_RANDOM can only be applied to BIGINT PRIMARY KEY columns")
}

func TestForeignKeyTypeMismatch(t *testing.T) {
	db := &schema.Database{
		Name:    "app",
		Dialect: schema.DialectMySQL,
		Tables: []*schema.Table{
			{
				Name: "users",
				Columns: []*schema.Column{
					{Name: "id", Type: schema.DataTypeInt, PrimaryKey: true},
					{Name: "group_id", Type: schema.DataTypeString, References: "groups.id"},
				},
			},
			{
				Name: "groups",
				Columns: []*schema.Column{
					{Name: "id", Type: schema.DataTypeInt, PrimaryKey: true},
				},
			},
		},
	}

	err := Database(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "type mismatch between referencing column \"group_id\"")
}

func TestRawTypeInvalid(t *testing.T) {
	db := &schema.Database{
		Name:    "app",
		Dialect: schema.DialectMySQL,
		Tables: []*schema.Table{
			{
				Name: "users",
				Columns: []*schema.Column{
					{
						Name:    "id",
						Type:    schema.DataTypeInt,
						RawType: "JSONB",
					},
				},
			},
		},
	}

	err := Database(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "is not a valid type for dialect \"mysql\"")
}

func TestRawTypeValid(t *testing.T) {
	db := &schema.Database{
		Name:    "app",
		Dialect: schema.DialectMySQL,
		Tables: []*schema.Table{
			{
				Name: "users",
				Columns: []*schema.Column{
					{
						Name:    "id",
						Type:    schema.DataTypeInt,
						RawType: "BIGINT",
					},
				},
			},
		},
	}

	err := Database(db)
	require.NoError(t, err)
}
