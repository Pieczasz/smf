package toml

import (
	"smf/internal/schema"
)

// tomlConstraint maps [[tables.constraints]].
type tomlConstraint struct {
	Name              string   `toml:"name"`
	Type              string   `toml:"type"`
	Columns           []string `toml:"columns"`
	ReferencedTable   string   `toml:"referenced_table"`
	ReferencedColumns []string `toml:"referenced_columns"`
	OnDelete          string   `toml:"on_delete"`
	OnUpdate          string   `toml:"on_update"`
	CheckExpression   string   `toml:"check_expression"`
	Enforced          *bool    `toml:"enforced"` // pointer: absent -> true/not supported
}

func constraint(tc *tomlConstraint) *schema.Constraint {
	c := &schema.Constraint{
		Name:              tc.Name,
		Type:              schema.ConstraintType(tc.Type),
		Columns:           tc.Columns,
		ReferencedTable:   tc.ReferencedTable,
		ReferencedColumns: tc.ReferencedColumns,
		OnDelete:          schema.ReferentialAction(tc.OnDelete),
		OnUpdate:          schema.ReferentialAction(tc.OnUpdate),
		CheckExpression:   tc.CheckExpression,
	}

	if tc.Enforced != nil {
		c.Enforced = tc.Enforced
	} else {
		enforced := true
		c.Enforced = &enforced
	}

	return c
}
