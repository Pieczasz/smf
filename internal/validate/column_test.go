package validate

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"smf/internal/schema"
)

func TestColumnInvalidReferencesFormat(t *testing.T) {
	db := &schema.Database{
		Name:    "app",
		Dialect: schema.DialectMySQL,
		Tables: []*schema.Table{
			{
				Name: "users",
				Columns: []*schema.Column{
					{Name: "role_id", Type: schema.DataTypeInt, References: "roles"},
				},
			},
		},
	}

	err := Database(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `invalid references "roles"`)
}

func TestColumnEmptyType(t *testing.T) {
	tests := []struct {
		name string
		col  *schema.Column
	}{
		{
			name: "empty type and rawtype",
			col:  &schema.Column{Name: "id"},
		},
		{
			name: "unknown type",
			col:  &schema.Column{Name: "id", Type: schema.DataTypeUnknown},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := &schema.Database{
				Name:    "app",
				Dialect: schema.DialectMySQL,
				Tables: []*schema.Table{
					{
						Name:    "users",
						Columns: []*schema.Column{tt.col},
					},
				},
			}

			err := Database(db)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "type is empty")
		})
	}
}

func TestColumnValid(t *testing.T) {
	db := &schema.Database{
		Name:    "app",
		Dialect: schema.DialectMySQL,
		Tables: []*schema.Table{
			{
				Name: "users",
				Columns: []*schema.Column{
					{Name: "id", Type: schema.DataTypeInt},
				},
			},
		},
	}

	err := Database(db)
	require.NoError(t, err)
}

func TestColumnValidReferences(t *testing.T) {
	db := &schema.Database{
		Name:    "app",
		Dialect: schema.DialectMySQL,
		Tables: []*schema.Table{
			{
				Name: "users",
				Columns: []*schema.Column{
					{Name: "id", Type: schema.DataTypeInt, PrimaryKey: true},
					{Name: "role_id", Type: schema.DataTypeInt, References: "roles.id"},
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
}
