package validate

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"smf/internal/schema"
)

func TestTimestampsDisabled(t *testing.T) {
	db := &schema.Database{
		Name:    "app",
		Dialect: schema.DialectMySQL,
		Tables: []*schema.Table{
			{
				Name:       "users",
				Columns:    []*schema.Column{{Name: "id", Type: schema.DataTypeInt}},
				Timestamps: &schema.TimestampsConfig{Enabled: false},
			},
		},
	}

	err := Database(db)
	require.NoError(t, err)
}

func TestTimestampsDefaultDistinctNames(t *testing.T) {
	db := &schema.Database{
		Name:    "app",
		Dialect: schema.DialectMySQL,
		Tables: []*schema.Table{
			{
				Name:       "users",
				Columns:    []*schema.Column{{Name: "id", Type: schema.DataTypeInt}},
				Timestamps: &schema.TimestampsConfig{Enabled: true},
			},
		},
	}

	err := Database(db)
	require.NoError(t, err)
}

func TestTimestampsSameNames(t *testing.T) {
	db := &schema.Database{
		Name:    "app",
		Dialect: schema.DialectMySQL,
		Tables: []*schema.Table{
			{
				Name:    "users",
				Columns: []*schema.Column{{Name: "id", Type: schema.DataTypeInt}},
				Timestamps: &schema.TimestampsConfig{
					Enabled:       true,
					CreatedColumn: "created_at",
					UpdatedColumn: "created_at",
				},
			},
		},
	}

	err := Database(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "resolve to the same name")
}

func TestTimestampsCustomColumnValid(t *testing.T) {
	db := &schema.Database{
		Name:    "app",
		Dialect: schema.DialectMySQL,
		Tables: []*schema.Table{
			{
				Name:    "users",
				Columns: []*schema.Column{{Name: "id", Type: schema.DataTypeInt}},
				Timestamps: &schema.TimestampsConfig{
					Enabled:       true,
					CreatedColumn: "creation_date",
					UpdatedColumn: "last_update",
				},
			},
		},
	}

	err := Database(db)
	require.NoError(t, err)
}

func TestTimestampsCustomColumnInvalidName(t *testing.T) {
	db := &schema.Database{
		Name:    "app",
		Dialect: schema.DialectMySQL,
		Tables: []*schema.Table{
			{
				Name:    "users",
				Columns: []*schema.Column{{Name: "id", Type: schema.DataTypeInt}},
				Timestamps: &schema.TimestampsConfig{
					Enabled:       true,
					CreatedColumn: "CreatedAt",
				},
			},
		},
	}

	err := Database(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must be in snake_case")
}
