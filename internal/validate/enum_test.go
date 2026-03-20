package validate

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"smf/internal/schema"
)

func TestValidateEnumsColumnType(t *testing.T) {
	tests := []struct {
		name    string
		db      *schema.Database
		wantErr string
	}{
		{
			name: "invalid column type",
			db: &schema.Database{
				Name:    "app",
				Dialect: schema.DialectMySQL,
				Tables: []*schema.Table{
					{
						Name: "users",
						Columns: []*schema.Column{
							{Name: "id", Type: "BANANA"},
						},
					},
				},
			},
			wantErr: "invalid type \"BANANA\"",
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

func TestValidateEnumsColumnRefActions(t *testing.T) {
	tests := []struct {
		name    string
		db      *schema.Database
		wantErr string
	}{
		{
			name: "invalid ref_on_delete",
			db: &schema.Database{
				Name:    "app",
				Dialect: schema.DialectMySQL,
				Tables: []*schema.Table{
					{
						Name: "users",
						Columns: []*schema.Column{
							{Name: "id", Type: schema.DataTypeInt, PrimaryKey: true},
							{Name: "role_id", Type: schema.DataTypeInt, References: "roles.id", RefOnDelete: "OOPS"},
						},
					},
					{
						Name: "roles",
						Columns: []*schema.Column{
							{Name: "id", Type: schema.DataTypeInt, PrimaryKey: true},
						},
					},
				},
			},
			wantErr: "invalid ref_on_delete \"OOPS\"",
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

func TestEnumsColumnType(t *testing.T) {
	tests := []struct {
		name    string
		col     *schema.Column
		table   *schema.Table
		wantErr string
	}{
		{
			name:    "invalid type",
			col:     &schema.Column{Name: "id", Type: "BANANA"},
			table:   &schema.Table{Name: "users"},
			wantErr: "invalid type",
		},
		{
			name:    "valid string type",
			col:     &schema.Column{Name: "name", Type: schema.DataTypeString},
			table:   &schema.Table{Name: "users"},
			wantErr: "",
		},
		{
			name:    "valid int type",
			col:     &schema.Column{Name: "id", Type: schema.DataTypeInt},
			table:   &schema.Table{Name: "users"},
			wantErr: "",
		},
		{
			name:    "valid bool type",
			col:     &schema.Column{Name: "active", Type: schema.DataTypeBoolean},
			table:   &schema.Table{Name: "users"},
			wantErr: "",
		},
		{
			name:    "valid datetime type",
			col:     &schema.Column{Name: "created_at", Type: schema.DataTypeDatetime},
			table:   &schema.Table{Name: "users"},
			wantErr: "",
		},
		{
			name:    "valid json type",
			col:     &schema.Column{Name: "data", Type: schema.DataTypeJSON},
			table:   &schema.Table{Name: "users"},
			wantErr: "",
		},
		{
			name:    "valid uuid type",
			col:     &schema.Column{Name: "uuid", Type: schema.DataTypeUUID},
			table:   &schema.Table{Name: "users"},
			wantErr: "",
		},
		{
			name:    "valid binary type",
			col:     &schema.Column{Name: "data", Type: schema.DataTypeBinary},
			table:   &schema.Table{Name: "users"},
			wantErr: "",
		},
		{
			name:    "valid enum type",
			col:     &schema.Column{Name: "status", Type: schema.DataTypeEnum},
			table:   &schema.Table{Name: "users"},
			wantErr: "",
		},
		{
			name:    "empty type is valid (handled elsewhere)",
			col:     &schema.Column{Name: "id"},
			table:   &schema.Table{Name: "users"},
			wantErr: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ColumnType(tt.col, tt.table)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestEnumsRefActions(t *testing.T) {
	tests := []struct {
		name    string
		col     *schema.Column
		table   *schema.Table
		wantErr string
	}{
		{
			name:    "invalid ref_on_delete",
			col:     &schema.Column{Name: "id", RefOnDelete: "INVALID"},
			table:   &schema.Table{Name: "users"},
			wantErr: "invalid ref_on_delete",
		},
		{
			name:    "invalid ref_on_update",
			col:     &schema.Column{Name: "id", RefOnUpdate: "INVALID"},
			table:   &schema.Table{Name: "users"},
			wantErr: "invalid ref_on_update",
		},
		{
			name:    "valid ref actions",
			col:     &schema.Column{Name: "id", RefOnDelete: schema.RefActionCascade, RefOnUpdate: schema.RefActionSetNull},
			table:   &schema.Table{Name: "users"},
			wantErr: "",
		},
		{
			name:    "empty ref actions valid",
			col:     &schema.Column{Name: "id"},
			table:   &schema.Table{Name: "users"},
			wantErr: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := RefActions(tt.col, tt.table)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestEnumsGeneration(t *testing.T) {
	tests := []struct {
		name    string
		col     *schema.Column
		table   *schema.Table
		wantErr string
	}{
		{
			name:    "invalid generation_storage",
			col:     &schema.Column{Name: "id", IsGenerated: true, GenerationStorage: "INVALID"},
			table:   &schema.Table{Name: "users"},
			wantErr: "invalid generation_storage",
		},
		{
			name:    "valid virtual",
			col:     &schema.Column{Name: "id", IsGenerated: true, GenerationStorage: schema.GenerationVirtual},
			table:   &schema.Table{Name: "users"},
			wantErr: "",
		},
		{
			name:    "valid stored",
			col:     &schema.Column{Name: "id", IsGenerated: true, GenerationStorage: schema.GenerationStored},
			table:   &schema.Table{Name: "users"},
			wantErr: "",
		},
		{
			name:    "not generated - no error",
			col:     &schema.Column{Name: "id", GenerationStorage: "SOMEVALUE"},
			table:   &schema.Table{Name: "users"},
			wantErr: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Generation(tt.col, tt.table)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestEnumsIdentity(t *testing.T) {
	tests := []struct {
		name    string
		col     *schema.Column
		table   *schema.Table
		wantErr string
	}{
		{
			name:    "invalid identity_generation",
			col:     &schema.Column{Name: "id", IdentityGeneration: "INVALID"},
			table:   &schema.Table{Name: "users"},
			wantErr: "invalid identity_generation",
		},
		{
			name:    "valid always",
			col:     &schema.Column{Name: "id", IdentityGeneration: schema.IdentityAlways},
			table:   &schema.Table{Name: "users"},
			wantErr: "",
		},
		{
			name:    "valid by default",
			col:     &schema.Column{Name: "id", IdentityGeneration: schema.IdentityByDefault},
			table:   &schema.Table{Name: "users"},
			wantErr: "",
		},
		{
			name:    "empty identity valid",
			col:     &schema.Column{Name: "id"},
			table:   &schema.Table{Name: "users"},
			wantErr: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Identity(tt.col, tt.table)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestConstraintEnumsInvalid(t *testing.T) {
	tests := []struct {
		name    string
		con     *schema.Constraint
		table   *schema.Table
		wantErr string
	}{
		{
			name:    "invalid constraint type",
			con:     &schema.Constraint{Name: "invalid", Type: "INVALID"},
			table:   &schema.Table{Name: "users"},
			wantErr: "invalid constraint type",
		},
		{
			name:    "invalid on_delete",
			con:     &schema.Constraint{Name: "fk", Type: schema.ConstraintForeignKey, OnDelete: "INVALID"},
			table:   &schema.Table{Name: "users"},
			wantErr: "invalid on_delete",
		},
		{
			name:    "invalid on_update",
			con:     &schema.Constraint{Name: "fk", Type: schema.ConstraintForeignKey, OnUpdate: "INVALID"},
			table:   &schema.Table{Name: "users"},
			wantErr: "invalid on_update",
		},
		{
			name:    "valid primary key",
			con:     &schema.Constraint{Name: "pk", Type: schema.ConstraintPrimaryKey},
			table:   &schema.Table{Name: "users"},
			wantErr: "",
		},
		{
			name:    "valid unique",
			con:     &schema.Constraint{Name: "uq", Type: schema.ConstraintUnique},
			table:   &schema.Table{Name: "users"},
			wantErr: "",
		},
		{
			name:    "valid check",
			con:     &schema.Constraint{Name: "chk", Type: schema.ConstraintCheck},
			table:   &schema.Table{Name: "users"},
			wantErr: "",
		},
		{
			name:    "valid FK with actions",
			con:     &schema.Constraint{Name: "fk", Type: schema.ConstraintForeignKey, OnDelete: schema.RefActionCascade, OnUpdate: schema.RefActionRestrict},
			table:   &schema.Table{Name: "users"},
			wantErr: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ConstraintEnums(tt.con, tt.table)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestIndexEnumsInvalidType(t *testing.T) {
	err := IndexEnums(&schema.Index{Name: "idx", Type: "INVALID"}, &schema.Table{Name: "users"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid index type")
}

func TestIndexEnumsInvalidVisibility(t *testing.T) {
	err := IndexEnums(&schema.Index{Name: "idx", Visibility: "INVALID"}, &schema.Table{Name: "users"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid visibility")
}

func TestIndexEnumsInvalidSortOrder(t *testing.T) {
	err := IndexEnums(&schema.Index{Name: "idx", Columns: []schema.ColumnIndex{{Name: "id", Order: "INVALID"}}}, &schema.Table{Name: "users"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid sort order")
}

func TestIndexEnumsValidTypes(t *testing.T) {
	tests := []*schema.Index{
		{Name: "idx", Type: schema.IndexTypeBTree},
		{Name: "idx", Type: schema.IndexTypeHash},
		{Name: "idx", Type: schema.IndexTypeFullText},
		{Name: "idx", Type: schema.IndexTypeSpatial},
		{Name: "idx", Type: schema.IndexTypeGIN},
		{Name: "idx", Type: schema.IndexTypeGiST},
		{Name: "idx", Type: ""},
	}

	for _, idx := range tests {
		err := IndexEnums(idx, &schema.Table{Name: "users"})
		assert.NoError(t, err)
	}
}

func TestIndexEnumsValidVisibility(t *testing.T) {
	tests := []*schema.Index{
		{Name: "idx", Visibility: schema.IndexVisible},
		{Name: "idx", Visibility: schema.IndexInvisible},
		{Name: "idx", Visibility: ""},
	}

	for _, idx := range tests {
		err := IndexEnums(idx, &schema.Table{Name: "users"})
		assert.NoError(t, err)
	}
}

func TestIndexEnumsValidOrder(t *testing.T) {
	tests := []struct {
		idx *schema.Index
	}{
		{idx: &schema.Index{Name: "idx", Columns: []schema.ColumnIndex{{Name: "id", Order: schema.SortAsc}}}},
		{idx: &schema.Index{Name: "idx", Columns: []schema.ColumnIndex{{Name: "id", Order: schema.SortDesc}}}},
		{idx: &schema.Index{Name: "idx", Columns: []schema.ColumnIndex{{Name: "id", Order: ""}}}},
	}

	for _, tt := range tests {
		err := IndexEnums(tt.idx, &schema.Table{Name: "users"})
		assert.NoError(t, err)
	}
}
