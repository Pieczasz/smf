package core

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateSemanticAutoIncrement(t *testing.T) {
	tests := []struct {
		name    string
		db      *Database
		wantErr string
	}{
		{
			name: "auto_increment on non-integer",
			db: &Database{
				Name:    "app",
				Dialect: new(DialectMySQL),
				Tables: []*Table{
					{
						Name: "users",
						Columns: []*Column{
							{Name: "id", Type: DataTypeString, AutoIncrement: true},
						},
					},
				},
			},
			wantErr: "auto_increment is only allowed on integer columns",
		},
		{
			name: "SQLite auto_increment on non-PK",
			db: &Database{
				Name:    "app",
				Dialect: new(DialectSQLite),
				Tables: []*Table{
					{
						Name: "users",
						Columns: []*Column{
							{Name: "id", Type: DataTypeInt, AutoIncrement: true},
						},
					},
				},
			},
			wantErr: "SQLite AUTOINCREMENT is only allowed on PRIMARY KEY columns",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateDatabase(tt.db)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidateSemanticAutoIncrementValidTypes(t *testing.T) {
	tests := []struct {
		name string
		db   *Database
	}{
		{
			name: "valid auto_increment on bigint (DataTypeInt)",
			db: &Database{
				Name:    "app",
				Dialect: new(DialectMySQL),
				Tables: []*Table{
					{
						Name: "users",
						Columns: []*Column{
							{Name: "id", Type: DataTypeInt, AutoIncrement: true, PrimaryKey: true},
						},
					},
				},
			},
		},
		{
			name: "valid auto_increment on smallint (DataTypeInt)",
			db: &Database{
				Name:    "app",
				Dialect: new(DialectMySQL),
				Tables: []*Table{
					{
						Name: "users",
						Columns: []*Column{
							{Name: "id", Type: DataTypeInt, AutoIncrement: true, PrimaryKey: true},
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateDatabase(tt.db)
			assert.NoError(t, err)
		})
	}
}

func TestValidateSemanticPKAndGenerated(t *testing.T) {
	tests := []struct {
		name    string
		db      *Database
		wantErr string
	}{
		{
			name: "nullable PK on column level",
			db: &Database{
				Name:    "app",
				Dialect: new(DialectMySQL),
				Tables: []*Table{
					{
						Name: "users",
						Columns: []*Column{
							{Name: "id", Type: DataTypeInt, PrimaryKey: true, Nullable: true},
						},
					},
				},
			},
			wantErr: "primary key columns cannot be nullable",
		},
		{
			name: "nullable PK on table level",
			db: &Database{
				Name:    "app",
				Dialect: new(DialectMySQL),
				Tables: []*Table{
					{
						Name: "users",
						Columns: []*Column{
							{Name: "id", Type: DataTypeInt, Nullable: true},
						},
						Constraints: []*Constraint{
							{
								Type:    ConstraintPrimaryKey,
								Columns: []string{"id"},
							},
						},
					},
				},
			},
			wantErr: "primary key columns cannot be nullable",
		},
		{
			name: "generated column without expression",
			db: &Database{
				Name:    "app",
				Dialect: new(DialectMySQL),
				Tables: []*Table{
					{
						Name: "users",
						Columns: []*Column{
							{Name: "id", Type: DataTypeInt, PrimaryKey: true},
							{Name: "full_name", Type: DataTypeString, IsGenerated: true},
						},
					},
				},
			},
			wantErr: "generated column must have an expression",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateDatabase(tt.db)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidateSemanticIdentity(t *testing.T) {
	tests := []struct {
		name    string
		db      *Database
		wantErr string
	}{
		{
			name: "identity options on non-auto_increment column",
			db: &Database{
				Name:    "app",
				Dialect: new(DialectMySQL),
				Tables: []*Table{
					{
						Name: "users",
						Columns: []*Column{
							{Name: "id", Type: DataTypeInt, PrimaryKey: true, IdentitySeed: 100},
						},
					},
				},
			},
			wantErr: "identity_seed and identity_increment can only be set for auto_increment columns",
		},
		{
			name: "TiDB auto_random on non-PK",
			db: &Database{
				Name:    "app",
				Dialect: new(DialectTiDB),
				Tables: []*Table{
					{
						Name: "users",
						Columns: []*Column{
							{
								Name: "id",
								Type: DataTypeInt,
								TiDB: &TiDBColumnOptions{AutoRandom: 5},
							},
						},
					},
				},
			},
			wantErr: "TiDB AUTO_RANDOM can only be applied to BIGINT PRIMARY KEY columns",
		},
		{
			name: "TiDB auto_random on non-integer",
			db: &Database{
				Name:    "app",
				Dialect: new(DialectTiDB),
				Tables: []*Table{
					{
						Name: "users",
						Columns: []*Column{
							{
								Name:       "id",
								Type:       DataTypeString,
								PrimaryKey: true,
								TiDB:       &TiDBColumnOptions{AutoRandom: 5},
							},
						},
					},
				},
			},
			wantErr: "TiDB AUTO_RANDOM can only be applied to BIGINT PRIMARY KEY columns",
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

func TestValidateSemanticFKMismatch(t *testing.T) {
	db := &Database{
		Name:    "app",
		Dialect: new(DialectMySQL),
		Tables: []*Table{
			{
				Name: "users",
				Columns: []*Column{
					{Name: "id", Type: DataTypeInt, PrimaryKey: true},
					{Name: "group_id", Type: DataTypeString, References: "groups.id"},
				},
			},
			{
				Name: "groups",
				Columns: []*Column{
					{Name: "id", Type: DataTypeInt, PrimaryKey: true},
				},
			},
		},
	}

	err := ValidateDatabase(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "type mismatch between referencing column \"group_id\" (string) and referenced column \"id\" (int)")
}
