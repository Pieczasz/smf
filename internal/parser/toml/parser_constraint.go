package toml

import (
	"errors"
	"fmt"
	"strings"

	"smf/internal/core"
)

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

func checkPKConflict(table *core.Table) error {
	hasColumnPK := false
	for _, col := range table.Columns {
		if col.PrimaryKey {
			hasColumnPK = true
			break
		}
	}
	constraintPKCount := 0
	for _, con := range table.Constraints {
		if con.Type == core.ConstraintPrimaryKey {
			constraintPKCount++
		}
	}
	if constraintPKCount > 1 {
		return errors.New(
			"multiple PRIMARY KEY constraints declared; a table can have at most one primary key",
		)
	}
	if hasColumnPK && constraintPKCount > 0 {
		return errors.New(
			"primary key declared on both column(s) and in constraints section; " +
				"use column-level primary_key for single-column PKs or a constraint for composite PKs, not both",
		)
	}
	return nil
}

func synthesizeConstraints(table *core.Table) {
	synthesizePK(table)
	synthesizeUniqueConstraints(table)
	synthesizeCheckConstraints(table)
	synthesizeFKConstraints(table)
}

func synthesizePK(table *core.Table) {
	for _, con := range table.Constraints {
		if con.Type == core.ConstraintPrimaryKey {
			return
		}
	}

	var pkCols []string
	for _, col := range table.Columns {
		if col.PrimaryKey {
			pkCols = append(pkCols, col.Name)
		}
	}
	if len(pkCols) == 0 {
		return
	}

	name := core.AutoGenerateConstraintName(core.ConstraintPrimaryKey, table.Name, pkCols, "")
	table.Constraints = append(table.Constraints, &core.Constraint{
		Name:    name,
		Type:    core.ConstraintPrimaryKey,
		Columns: pkCols,
	})
}

func synthesizeUniqueConstraints(table *core.Table) {
	for _, col := range table.Columns {
		if !col.Unique {
			continue
		}
		cols := []string{col.Name}
		name := core.AutoGenerateConstraintName(core.ConstraintUnique, table.Name, cols, "")
		table.Constraints = append(table.Constraints, &core.Constraint{
			Name:    name,
			Type:    core.ConstraintUnique,
			Columns: cols,
		})
	}
}

func synthesizeCheckConstraints(table *core.Table) {
	for _, col := range table.Columns {
		if col.Check == "" {
			continue
		}
		cols := []string{col.Name}
		name := core.AutoGenerateConstraintName(core.ConstraintCheck, table.Name, cols, "")
		table.Constraints = append(table.Constraints, &core.Constraint{
			Name:            name,
			Type:            core.ConstraintCheck,
			CheckExpression: col.Check,
			Enforced:        true,
		})
	}
}

func synthesizeFKConstraints(table *core.Table) {
	for _, col := range table.Columns {
		if col.References == "" {
			continue
		}
		// ParseReferences is guaranteed to succeed here because
		// convertColumn already validated the format.
		refTable, refCol, _ := core.ParseReferences(col.References)
		cols := []string{col.Name}
		name := core.AutoGenerateConstraintName(core.ConstraintForeignKey, table.Name, cols, refTable)
		table.Constraints = append(table.Constraints, &core.Constraint{
			Name:              name,
			Type:              core.ConstraintForeignKey,
			Columns:           cols,
			ReferencedTable:   refTable,
			ReferencedColumns: []string{refCol},
			OnDelete:          col.RefOnDelete,
			OnUpdate:          col.RefOnUpdate,
			Enforced:          true,
		})
	}
}

// validateConstraints checks for duplicate names, missing columns, and
// incomplete FK definitions across all constraints in the table.
func validateConstraints(table *core.Table) error {
	seen := make(map[string]bool, len(table.Constraints))
	for _, con := range table.Constraints {
		if con.Name == "" {
			continue
		}
		lower := strings.ToLower(con.Name)
		if seen[lower] {
			return fmt.Errorf("duplicate constraint name %q", con.Name)
		}
		seen[lower] = true
	}

	for _, con := range table.Constraints {
		if err := validateConstraintColumns(table, con); err != nil {
			return err
		}
	}

	return nil
}

// validateConstraintColumns verifies that a single constraint's column list
// is non-empty (except for CHECK), that every referenced column exists, and
// that FK constraints carry the required referenced_table / referenced_columns.
func validateConstraintColumns(table *core.Table, con *core.Constraint) error {
	if con.Type == core.ConstraintCheck {
		return nil // CHECK constraints use expressions, not column lists.
	}
	if len(con.Columns) == 0 {
		return fmt.Errorf("constraint %q (%s) has no columns", con.Name, con.Type)
	}
	for _, colName := range con.Columns {
		if table.FindColumn(colName) == nil {
			return fmt.Errorf("constraint %q references nonexistent column %q", con.Name, colName)
		}
	}
	if con.Type == core.ConstraintForeignKey {
		if con.ReferencedTable == "" {
			return fmt.Errorf("foreign key constraint %q is missing referenced_table", con.Name)
		}
		if len(con.ReferencedColumns) == 0 {
			return fmt.Errorf("foreign key constraint %q is missing referenced_columns", con.Name)
		}
	}
	return nil
}
