package core

import (
	"fmt"
	"strings"
)

// validateConstraints checks for duplicate constraint names, missing columns,
// and incomplete FK definitions.
func validateConstraints(table *Table) error {
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

// validateConstraintColumns verifies a single constraint's columns exist, are
// non-empty (except CHECK), and that FK constraints have referenced_table and
// referenced_columns.
func validateConstraintColumns(table *Table, con *Constraint) error {
	if con.Type == ConstraintCheck {
		return nil
	}
	if len(con.Columns) == 0 {
		return fmt.Errorf("constraint %q (%s) has no columns", con.Name, con.Type)
	}
	for _, colName := range con.Columns {
		if table.FindColumn(colName) == nil {
			return fmt.Errorf("constraint %q references nonexistent column %q", con.Name, colName)
		}
	}
	if con.Type == ConstraintForeignKey {
		if con.ReferencedTable == "" {
			return fmt.Errorf("foreign key constraint %q is missing referenced_table", con.Name)
		}
		if len(con.ReferencedColumns) == 0 {
			return fmt.Errorf("foreign key constraint %q is missing referenced_columns", con.Name)
		}
	}
	return nil
}

func validateFKColumnExistence(db *Database, tableMap map[string]*Table) error {
	for _, t := range db.Tables {
		for _, con := range t.Constraints {
			if con.Type != ConstraintForeignKey {
				continue
			}

			refTable, exists := tableMap[strings.ToLower(con.ReferencedTable)]
			if !exists {
				return fmt.Errorf("constraint %q in table %q references non-existent table %q",
					con.Name, t.Name, con.ReferencedTable)
			}

			for _, refColName := range con.ReferencedColumns {
				if refTable.FindColumn(refColName) == nil {
					return fmt.Errorf("constraint %q in table %q references non-existent column %q in table %q",
						con.Name, t.Name, refColName, con.ReferencedTable)
				}
			}
		}
	}

	return nil
}

// synthesizeConstraints generates constraint objects from column-level
// shortcuts (primary_key, unique, check, references). It should be called
// after PK conflict validation and before structural constraint validation.
func synthesizeConstraints(table *Table) {
	synthesizePK(table)
	synthesizeUniqueConstraints(table)
	synthesizeCheckConstraints(table)
	synthesizeFKConstraints(table)
}

func synthesizePK(table *Table) {
	for _, con := range table.Constraints {
		if con.Type == ConstraintPrimaryKey {
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

	name := AutoGenerateConstraintName(ConstraintPrimaryKey, table.Name, pkCols, "")
	table.Constraints = append(table.Constraints, &Constraint{
		Name:    name,
		Type:    ConstraintPrimaryKey,
		Columns: pkCols,
	})
}

func synthesizeUniqueConstraints(table *Table) {
	for _, col := range table.Columns {
		if !col.Unique {
			continue
		}
		cols := []string{col.Name}
		name := AutoGenerateConstraintName(ConstraintUnique, table.Name, cols, "")
		table.Constraints = append(table.Constraints, &Constraint{
			Name:    name,
			Type:    ConstraintUnique,
			Columns: cols,
		})
	}
}

func synthesizeCheckConstraints(table *Table) {
	for _, col := range table.Columns {
		if col.Check == "" {
			continue
		}
		cols := []string{col.Name}
		name := AutoGenerateConstraintName(ConstraintCheck, table.Name, cols, "")
		table.Constraints = append(table.Constraints, &Constraint{
			Name:            name,
			Type:            ConstraintCheck,
			CheckExpression: col.Check,
			Enforced:        true,
		})
	}
}

func synthesizeFKConstraints(table *Table) {
	for _, col := range table.Columns {
		if col.References == "" {
			continue
		}
		refTable, refCol, ok := ParseReferences(col.References)
		if !ok {
			continue
		}
		cols := []string{col.Name}
		name := AutoGenerateConstraintName(ConstraintForeignKey, table.Name, cols, refTable)
		table.Constraints = append(table.Constraints, &Constraint{
			Name:              name,
			Type:              ConstraintForeignKey,
			Columns:           cols,
			ReferencedTable:   refTable,
			ReferencedColumns: []string{refCol},
			OnDelete:          col.RefOnDelete,
			OnUpdate:          col.RefOnUpdate,
			Enforced:          true,
		})
	}
}
