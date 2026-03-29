package mysql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"smf/internal/schema"
)

func TestParseConstraint_PrimaryKey(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected *schema.Constraint
		wantErr  bool
	}{
		{
			name:  "Primary Key Simple",
			input: "PRIMARY KEY (`id`)",
			expected: &schema.Constraint{
				Type:     schema.ConstraintPrimaryKey,
				Columns:  []string{"id"},
				Enforced: new(true),
			},
		},
		{
			name:  "Primary Key With Name and Multiple Columns",
			input: "CONSTRAINT `pk_users` PRIMARY KEY (`tenant_id`, `user_id`)",
			expected: &schema.Constraint{
				Name:     "pk_users",
				Type:     schema.ConstraintPrimaryKey,
				Columns:  []string{"tenant_id", "user_id"},
				Enforced: new(true),
			},
		},
		{
			name:  "Primary Key With Index Type",
			input: "PRIMARY KEY USING BTREE (`id`)",
			expected: &schema.Constraint{
				Type:     schema.ConstraintPrimaryKey,
				Columns:  []string{"id"},
				Enforced: new(true),
			},
		},
		{
			name:    "Missing Key in PK",
			input:   "PRIMARY (`id`)",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseConstraint(schema.DialectMySQL, tc.input)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.expected, got)
			}
		})
	}
}

func TestParseConstraint_Unique(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected *schema.Constraint
		wantErr  bool
	}{
		{
			name:  "Unique Simple",
			input: "UNIQUE (`email`)",
			expected: &schema.Constraint{
				Type:     schema.ConstraintUnique,
				Columns:  []string{"email"},
				Enforced: new(true),
			},
		},
		{
			name:  "Unique Key With Name",
			input: "UNIQUE KEY `uk_email` (`email`)",
			expected: &schema.Constraint{
				Name:     "uk_email",
				Type:     schema.ConstraintUnique,
				Columns:  []string{"email"},
				Enforced: new(true),
			},
		},
		{
			name:  "Unique Index Constraint Name",
			input: "CONSTRAINT `uniq_username` UNIQUE INDEX (`username`)",
			expected: &schema.Constraint{
				Name:     "uniq_username",
				Type:     schema.ConstraintUnique,
				Columns:  []string{"username"},
				Enforced: new(true),
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseConstraint(schema.DialectMySQL, tc.input)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.expected, got)
			}
		})
	}
}

func TestParseConstraint_ForeignKey_Basic(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected *schema.Constraint
		wantErr  bool
	}{
		{
			name:  "Foreign Key Simple",
			input: "FOREIGN KEY (`role_id`) REFERENCES `roles` (`id`)",
			expected: &schema.Constraint{
				Type:              schema.ConstraintForeignKey,
				Columns:           []string{"role_id"},
				ReferencedTable:   "roles",
				ReferencedColumns: []string{"id"},
				Enforced:          new(true),
			},
		},
		{
			name:  "Foreign Key With Match",
			input: "FOREIGN KEY (`role_id`) REFERENCES `roles` (`id`) MATCH FULL",
			expected: &schema.Constraint{
				Type:              schema.ConstraintForeignKey,
				Columns:           []string{"role_id"},
				ReferencedTable:   "roles",
				ReferencedColumns: []string{"id"},
				Match:             "FULL",
				Enforced:          new(true),
			},
		},
		{
			name:    "Missing Key in FK",
			input:   "FOREIGN (`id`)",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseConstraint(schema.DialectMySQL, tc.input)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.expected, got)
			}
		})
	}
}

func TestParseConstraint_ForeignKey_Actions(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected *schema.Constraint
		wantErr  bool
	}{
		{
			name:  "Foreign Key With Actions",
			input: "CONSTRAINT `fk_user` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`) ON DELETE CASCADE ON UPDATE RESTRICT",
			expected: &schema.Constraint{
				Name:              "fk_user",
				Type:              schema.ConstraintForeignKey,
				Columns:           []string{"user_id"},
				ReferencedTable:   "users",
				ReferencedColumns: []string{"id"},
				OnDelete:          schema.RefActionCascade,
				OnUpdate:          schema.RefActionRestrict,
				Enforced:          new(true),
			},
		},
		{
			name:  "Foreign Key With SET NULL and NO ACTION",
			input: "FOREIGN KEY (`col1`) REFERENCES `tbl` (`col2`) ON DELETE SET NULL ON UPDATE NO ACTION",
			expected: &schema.Constraint{
				Type:              schema.ConstraintForeignKey,
				Columns:           []string{"col1"},
				ReferencedTable:   "tbl",
				ReferencedColumns: []string{"col2"},
				OnDelete:          schema.RefActionSetNull,
				OnUpdate:          schema.RefActionNoAction,
				Enforced:          new(true),
			},
		},
		{
			name:  "Foreign Key With SET DEFAULT",
			input: "FOREIGN KEY (`col1`) REFERENCES `tbl` (`col2`) ON UPDATE SET DEFAULT",
			expected: &schema.Constraint{
				Type:              schema.ConstraintForeignKey,
				Columns:           []string{"col1"},
				ReferencedTable:   "tbl",
				ReferencedColumns: []string{"col2"},
				OnUpdate:          schema.RefActionSetDefault,
				Enforced:          new(true),
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseConstraint(schema.DialectMySQL, tc.input)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.expected, got)
			}
		})
	}
}

func TestParseConstraint_MissingBranches(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected *schema.Constraint
		wantErr  bool
	}{
		{
			name:    "Constraint without type",
			input:   "CONSTRAINT `foo`",
			wantErr: true,
		},
		{
			name:  "Foreign Key With Name Before Columns",
			input: "FOREIGN KEY `fk_name` (`role_id`) REFERENCES `roles` (`id`)",
			expected: &schema.Constraint{
				Name:              "fk_name",
				Type:              schema.ConstraintForeignKey,
				Columns:           []string{"role_id"},
				ReferencedTable:   "roles",
				ReferencedColumns: []string{"id"},
				Enforced:          new(true),
			},
		},
		{
			name:  "Match without Type",
			input: "FOREIGN KEY (`role_id`) REFERENCES `roles` (`id`) MATCH",
			expected: &schema.Constraint{
				Type:              schema.ConstraintForeignKey,
				Columns:           []string{"role_id"},
				ReferencedTable:   "roles",
				ReferencedColumns: []string{"id"},
				Enforced:          new(true),
			},
		},
		{
			name:  "Match Invalid Type",
			input: "FOREIGN KEY (`role_id`) REFERENCES `roles` (`id`) MATCH INVALID",
			expected: &schema.Constraint{
				Type:              schema.ConstraintForeignKey,
				Columns:           []string{"role_id"},
				ReferencedTable:   "roles",
				ReferencedColumns: []string{"id"},
				Enforced:          new(true),
			},
		},
		{
			name:  "ON without action",
			input: "FOREIGN KEY (`role_id`) REFERENCES `roles` (`id`) ON",
			expected: &schema.Constraint{
				Type:              schema.ConstraintForeignKey,
				Columns:           []string{"role_id"},
				ReferencedTable:   "roles",
				ReferencedColumns: []string{"id"},
				Enforced:          new(true),
			},
		},
		{
			name:  "ON DELETE without action",
			input: "FOREIGN KEY (`role_id`) REFERENCES `roles` (`id`) ON DELETE",
			expected: &schema.Constraint{
				Type:              schema.ConstraintForeignKey,
				Columns:           []string{"role_id"},
				ReferencedTable:   "roles",
				ReferencedColumns: []string{"id"},
				Enforced:          new(true),
			},
		},
		{
			name:  "ON DELETE INVALID",
			input: "FOREIGN KEY (`role_id`) REFERENCES `roles` (`id`) ON DELETE INVALID",
			expected: &schema.Constraint{
				Type:              schema.ConstraintForeignKey,
				Columns:           []string{"role_id"},
				ReferencedTable:   "roles",
				ReferencedColumns: []string{"id"},
				Enforced:          new(true),
			},
		},
		{
			name:  "ON UPDATE SET without sub",
			input: "FOREIGN KEY (`role_id`) REFERENCES `roles` (`id`) ON UPDATE SET",
			expected: &schema.Constraint{
				Type:              schema.ConstraintForeignKey,
				Columns:           []string{"role_id"},
				ReferencedTable:   "roles",
				ReferencedColumns: []string{"id"},
				Enforced:          new(true),
			},
		},
		{
			name:  "ON UPDATE NO without ACTION",
			input: "FOREIGN KEY (`role_id`) REFERENCES `roles` (`id`) ON UPDATE NO",
			expected: &schema.Constraint{
				Type:              schema.ConstraintForeignKey,
				Columns:           []string{"role_id"},
				ReferencedTable:   "roles",
				ReferencedColumns: []string{"id"},
				Enforced:          new(true),
			},
		},
		{
			name:  "ON UPDATE SET INVALID",
			input: "FOREIGN KEY (`role_id`) REFERENCES `roles` (`id`) ON UPDATE SET INVALID",
			expected: &schema.Constraint{
				Type:              schema.ConstraintForeignKey,
				Columns:           []string{"role_id"},
				ReferencedTable:   "roles",
				ReferencedColumns: []string{"id"},
				Enforced:          new(true),
			},
		},
		{
			name:  "ON UPDATE NO INVALID",
			input: "FOREIGN KEY (`role_id`) REFERENCES `roles` (`id`) ON UPDATE NO INVALID",
			expected: &schema.Constraint{
				Type:              schema.ConstraintForeignKey,
				Columns:           []string{"role_id"},
				ReferencedTable:   "roles",
				ReferencedColumns: []string{"id"},
				Enforced:          new(true),
			},
		},
		{
			name:  "REFERENCES without table",
			input: "FOREIGN KEY (`role_id`) REFERENCES",
			expected: &schema.Constraint{
				Type:     schema.ConstraintForeignKey,
				Columns:  []string{"role_id"},
				Enforced: new(true),
			},
		},
		{
			name:  "CHECK NOT without ENFORCED",
			input: "CHECK (`val` > 0) NOT",
			expected: &schema.Constraint{
				Type:            schema.ConstraintCheck,
				CheckExpression: "`val` > 0",
				Enforced:        new(true),
			},
		},
		{
			name:  "UNIQUE without columns",
			input: "UNIQUE KEY `uk_email`",
			expected: &schema.Constraint{
				Name:     "uk_email",
				Type:     schema.ConstraintUnique,
				Enforced: new(true),
			},
		},
		{
			name:    "Incomplete constraint definition",
			input:   "CONSTRAINT",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseConstraint(schema.DialectMySQL, tc.input)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.expected, got)
			}
		})
	}
}

func TestParseConstraint_Check(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected *schema.Constraint
		wantErr  bool
	}{
		{
			name:  "Check Simple",
			input: "CHECK (`age` > 18)",
			expected: &schema.Constraint{
				Type:            schema.ConstraintCheck,
				CheckExpression: "`age` > 18",
				Enforced:        new(true),
			},
		},
		{
			name:  "Check Not Enforced",
			input: "CONSTRAINT `chk_status` CHECK (`status` IN ('active', 'inactive')) NOT ENFORCED",
			expected: &schema.Constraint{
				Name:            "chk_status",
				Type:            schema.ConstraintCheck,
				CheckExpression: "`status` IN ('active', 'inactive')",
				Enforced:        new(false),
			},
		},
		{
			name:  "Check Enforced Explicit",
			input: "CHECK (`val` > 0) ENFORCED",
			expected: &schema.Constraint{
				Type:            schema.ConstraintCheck,
				CheckExpression: "`val` > 0",
				Enforced:        new(true),
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseConstraint(schema.DialectMySQL, tc.input)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.expected, got)
			}
		})
	}
}

func TestParseConstraint_Misc(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected *schema.Constraint
		wantErr  bool
	}{
		{
			name:    "Invalid Type",
			input:   "CONSTRAINT `x` INVALID (`id`)",
			wantErr: true,
		},
		{
			name:     "Empty",
			input:    "",
			expected: nil,
			wantErr:  false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseConstraint(schema.DialectMySQL, tc.input)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.expected, got)
			}
		})
	}
}
