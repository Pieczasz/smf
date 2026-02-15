package core

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateEnumsColumn(t *testing.T) {
	tests := []struct {
		name    string
		db      *Database
		wantErr string
	}{
		{
			name: "invalid column type",
			db: &Database{
				Name:    "app",
				Dialect: new(DialectMySQL),
				Tables: []*Table{
					{
						Name: "users",
						Columns: []*Column{
							{Name: "id", Type: "BANANA"},
						},
					},
				},
			},
			wantErr: "invalid type \"BANANA\"",
		},
		{
			name: "invalid ref_on_delete",
			db: &Database{
				Name:    "app",
				Dialect: new(DialectMySQL),
				Tables: []*Table{
					{
						Name: "users",
						Columns: []*Column{
							{Name: "id", Type: DataTypeInt, PrimaryKey: true},
							{Name: "role_id", Type: DataTypeInt, References: "roles.id", RefOnDelete: "OOPS"},
						},
					},
					{
						Name: "roles",
						Columns: []*Column{
							{Name: "id", Type: DataTypeInt, PrimaryKey: true},
						},
					},
				},
			},
			wantErr: "invalid ref_on_delete \"OOPS\"",
		},
		{
			name: "invalid generation_storage",
			db: &Database{
				Name:    "app",
				Dialect: new(DialectMySQL),
				Tables: []*Table{
					{
						Name: "users",
						Columns: []*Column{
							{Name: "id", Type: DataTypeInt, PrimaryKey: true},
							{
								Name:                 "full_name",
								Type:                 DataTypeString,
								IsGenerated:          true,
								GenerationExpression: "first_name || ' ' || last_name",
								GenerationStorage:    "SIDEWAYS",
							},
						},
					},
				},
			},
			wantErr: "invalid generation_storage \"SIDEWAYS\"",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateDatabase(tt.db)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestValidateEnumsConstraint(t *testing.T) {
	tests := []struct {
		name    string
		db      *Database
		wantErr string
	}{
		{
			name: "invalid constraint type",
			db: &Database{
				Name:    "app",
				Dialect: new(DialectMySQL),
				Tables: []*Table{
					{
						Name: "users",
						Columns: []*Column{
							{Name: "id", Type: DataTypeInt, PrimaryKey: true},
						},
						Constraints: []*Constraint{
							{
								Name:    "bad_con",
								Type:    "SUPER_UNIQUE",
								Columns: []string{"id"},
							},
						},
					},
				},
			},
			wantErr: "invalid constraint type \"SUPER_UNIQUE\"",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateDatabase(tt.db)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestValidateEnumsIndexType(t *testing.T) {
	tests := []struct {
		name    string
		db      *Database
		wantErr string
	}{
		{
			name: "invalid index type",
			db: &Database{
				Name:    "app",
				Dialect: new(DialectMySQL),
				Tables: []*Table{
					{
						Name: "users",
						Columns: []*Column{
							{Name: "id", Type: DataTypeInt, PrimaryKey: true},
						},
						Indexes: []*Index{
							{
								Name:    "idx_id",
								Type:    "MAGIC",
								Columns: []ColumnIndex{{Name: "id"}},
							},
						},
					},
				},
			},
			wantErr: "invalid index type \"MAGIC\"",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateDatabase(tt.db)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestValidateEnumsIndexVisibilityAndOrder(t *testing.T) {
	tests := []struct {
		name    string
		db      *Database
		wantErr string
	}{
		{
			name: "invalid index visibility",
			db: &Database{
				Name:    "app",
				Dialect: new(DialectMySQL),
				Tables: []*Table{
					{
						Name: "users",
						Columns: []*Column{
							{Name: "id", Type: DataTypeInt, PrimaryKey: true},
						},
						Indexes: []*Index{
							{
								Name:       "idx_id",
								Visibility: "GHOST",
								Columns:    []ColumnIndex{{Name: "id"}},
							},
						},
					},
				},
			},
			wantErr: "invalid visibility \"GHOST\"",
		},
		{
			name: "invalid sort order",
			db: &Database{
				Name:    "app",
				Dialect: new(DialectMySQL),
				Tables: []*Table{
					{
						Name: "users",
						Columns: []*Column{
							{Name: "id", Type: DataTypeInt, PrimaryKey: true},
						},
						Indexes: []*Index{
							{
								Name:    "idx_id",
								Columns: []ColumnIndex{{Name: "id", Order: "RANDOM"}},
							},
						},
					},
				},
			},
			wantErr: "invalid sort order \"RANDOM\"",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateDatabase(tt.db)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}
