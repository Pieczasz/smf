package toml

import (
	"smf/internal/core"
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
	Enforced          *bool    `toml:"enforced"` // pointer: absent -> true
}

func convertTableConstraint(tc *tomlConstraint) *core.Constraint {
	c := &core.Constraint{
		Name:              tc.Name,
		Type:              core.ConstraintType(tc.Type),
		Columns:           tc.Columns,
		ReferencedTable:   tc.ReferencedTable,
		ReferencedColumns: tc.ReferencedColumns,
		OnDelete:          core.ReferentialAction(tc.OnDelete),
		OnUpdate:          core.ReferentialAction(tc.OnUpdate),
		CheckExpression:   tc.CheckExpression,
	}

	if tc.Enforced != nil {
		c.Enforced = *tc.Enforced
	} else {
		c.Enforced = true
	}

	return c
}
