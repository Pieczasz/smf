package validate

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"smf/internal/schema"
)

func TestDatabaseRequiredFields(t *testing.T) {
	tests := []struct {
		name    string
		db      *schema.Database
		wantErr string
	}{
		{
			name:    "nil database",
			db:      nil,
			wantErr: "database is nil",
		},
		{
			name: "missing dialect",
			db: &schema.Database{
				Name:    "app",
				Dialect: schema.Dialect(""),
			},
			wantErr: "dialect is required",
		},
		{
			name: "invalid dialect",
			db: &schema.Database{
				Name:    "app",
				Dialect: schema.Dialect("mongo"),
			},
			wantErr: "unsupported dialect",
		},
		{
			name: "missing database name",
			db: &schema.Database{
				Name:    "",
				Dialect: schema.DialectMySQL,
			},
			wantErr: "database name is required",
		},
		{
			name: "empty tables",
			db: &schema.Database{
				Name:    "app",
				Dialect: schema.DialectMySQL,
				Tables:  []*schema.Table{},
			},
			wantErr: "schema is empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Database(tt.db)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestDatabaseValid(t *testing.T) {
	db := &schema.Database{
		Name:    "app",
		Dialect: schema.DialectMySQL,
		Tables: []*schema.Table{
			{
				Name: "users",
				Columns: []*schema.Column{
					{Name: "id", Type: schema.DataTypeInt, PrimaryKey: true},
				},
			},
		},
	}

	err := Database(db)
	require.NoError(t, err)
}

func TestDatabaseDuplicateTableNames(t *testing.T) {
	db := &schema.Database{
		Name:    "app",
		Dialect: schema.DialectMySQL,
		Tables: []*schema.Table{
			{Name: "users", Columns: []*schema.Column{{Name: "id", Type: schema.DataTypeInt}}},
			{Name: "users", Columns: []*schema.Column{{Name: "id", Type: schema.DataTypeInt}}},
		},
	}

	err := Database(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate table name")
}

func TestDatabaseInvalidAllowedNamePattern(t *testing.T) {
	db := &schema.Database{
		Name:    "app",
		Dialect: schema.DialectMySQL,
		Validation: &schema.ValidationRules{
			AllowedNamePattern: "(",
		},
		Tables: []*schema.Table{
			{Name: "users", Columns: []*schema.Column{{Name: "id", Type: schema.DataTypeInt}}},
		},
	}

	err := Database(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid allowed_name_pattern")
}

func TestTableName(t *testing.T) {
	tests := []struct {
		name    string
		db      *schema.Database
		wantErr string
	}{
		{
			name: "invalid table name - not snake_case",
			db: &schema.Database{
				Name:    "app",
				Dialect: schema.DialectMySQL,
				Tables: []*schema.Table{
					{Name: "Users", Columns: []*schema.Column{{Name: "id", Type: schema.DataTypeInt}}},
				},
			},
			wantErr: "must be in snake_case",
		},
		{
			name: "table name exceeds max length",
			db: &schema.Database{
				Name:    "app",
				Dialect: schema.DialectMySQL,
				Validation: &schema.ValidationRules{
					MaxTableNameLength: 3,
				},
				Tables: []*schema.Table{
					{Name: "users", Columns: []*schema.Column{{Name: "id", Type: schema.DataTypeInt}}},
				},
			},
			wantErr: "exceeds maximum length",
		},
		{
			name: "table name does not match allowed pattern",
			db: &schema.Database{
				Name:    "app",
				Dialect: schema.DialectMySQL,
				Validation: &schema.ValidationRules{
					AllowedNamePattern: "^u[a-z]+$",
				},
				Tables: []*schema.Table{
					{Name: "users", Columns: []*schema.Column{{Name: "id", Type: schema.DataTypeInt}}},
				},
			},
			wantErr: "does not match allowed pattern",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Database(tt.db)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}
