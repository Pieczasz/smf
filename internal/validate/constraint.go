package validate

import (
	"fmt"

	"smf/internal/schema"
)

func Constraints(t *schema.Table) error {
	if err := ConstraintNames(t); err != nil {
		return err
	}
	return ConstraintColumns(t)
}

func ConstraintNames(t *schema.Table) error {
	seen := make(map[string]bool, len(t.Constraints))
	for _, con := range t.Constraints {
		if con.Name == "" {
			continue
		}
		if err := Name(con.Name, nil, nil, false); err != nil {
			return fmt.Errorf("constraint %q: %w", con.Name, err)
		}
		if seen[con.Name] {
			return fmt.Errorf("duplicate constraint name %q", con.Name)
		}
		seen[con.Name] = true
	}
	return nil
}

func ConstraintColumns(t *schema.Table) error {
	for _, con := range t.Constraints {
		if err := SingleConstraintColumns(t, con); err != nil {
			return err
		}
	}
	return nil
}

func SingleConstraintColumns(t *schema.Table, con *schema.Constraint) error {
	if con.Type == schema.ConstraintCheck {
		return nil
	}
	if len(con.Columns) == 0 {
		return fmt.Errorf("constraint %q (%s) has no columns", con.Name, con.Type)
	}
	for _, colName := range con.Columns {
		if t.FindColumn(colName) == nil {
			return fmt.Errorf("constraint %q references nonexistent column %q", con.Name, colName)
		}
	}
	if con.Type == schema.ConstraintForeignKey {
		if con.ReferencedTable == "" {
			return fmt.Errorf("foreign key constraint %q is missing referenced_table", con.Name)
		}
		if len(con.ReferencedColumns) == 0 {
			return fmt.Errorf("foreign key constraint %q is missing referenced_columns", con.Name)
		}
	}
	return nil
}

func ForeignKeys(tables []*schema.Table) error {
	for _, t := range tables {
		for _, con := range t.Constraints {
			if con.Type != schema.ConstraintForeignKey {
				continue
			}
			refTable := FindTable(tables, con.ReferencedTable)
			if refTable == nil {
				return fmt.Errorf("table %q, constraint %q: references non-existent table %q",
					t.Name, con.Name, con.ReferencedTable)
			}
			for _, refColName := range con.ReferencedColumns {
				if refTable.FindColumn(refColName) == nil {
					return fmt.Errorf("table %q, constraint %q: references non-existent column %q in table %q",
						t.Name, con.Name, refColName, con.ReferencedTable)
				}
			}
			for _, colName := range con.Columns {
				if t.FindColumn(colName) == nil {
					return fmt.Errorf("table %q, constraint %q: references non-existent column %q",
						t.Name, con.Name, colName)
				}
			}
		}
	}
	return nil
}

func FindTable(tables []*schema.Table, name string) *schema.Table {
	for _, t := range tables {
		if t.Name == name {
			return t
		}
	}
	return nil
}

func Synthesize(t *schema.Table) {
	SynthesizePrimaryKey(t)
	SynthesizeUniqueConstraints(t)
	SynthesizeCheckConstraints(t)
	SynthesizeForeignKeyConstraints(t)
}

func SynthesizePrimaryKey(t *schema.Table) {
	for _, con := range t.Constraints {
		if con.Type == schema.ConstraintPrimaryKey {
			return
		}
	}

	var pkCols []string
	for _, col := range t.Columns {
		if col.PrimaryKey {
			pkCols = append(pkCols, col.Name)
		}
	}
	if len(pkCols) == 0 {
		return
	}

	name := schema.AutoGenerateConstraintName(schema.ConstraintPrimaryKey, t.Name, pkCols, "")
	t.Constraints = append(t.Constraints, &schema.Constraint{
		Name:    name,
		Type:    schema.ConstraintPrimaryKey,
		Columns: pkCols,
	})
}

func SynthesizeUniqueConstraints(t *schema.Table) {
	for _, col := range t.Columns {
		if !col.Unique {
			continue
		}
		cols := []string{col.Name}
		name := schema.AutoGenerateConstraintName(schema.ConstraintUnique, t.Name, cols, "")
		t.Constraints = append(t.Constraints, &schema.Constraint{
			Name:    name,
			Type:    schema.ConstraintUnique,
			Columns: cols,
		})
	}
}

func SynthesizeCheckConstraints(t *schema.Table) {
	for _, col := range t.Columns {
		if col.Check == "" {
			continue
		}
		cols := []string{col.Name}
		name := schema.AutoGenerateConstraintName(schema.ConstraintCheck, t.Name, cols, "")
		enforced := true
		t.Constraints = append(t.Constraints, &schema.Constraint{
			Name:            name,
			Type:            schema.ConstraintCheck,
			CheckExpression: col.Check,
			Enforced:        &enforced,
		})
	}
}

func SynthesizeForeignKeyConstraints(t *schema.Table) {
	for _, col := range t.Columns {
		if col.References == "" {
			continue
		}
		refTable, refCol, ok := schema.ParseReferences(col.References)
		if !ok {
			continue
		}
		cols := []string{col.Name}
		name := schema.AutoGenerateConstraintName(schema.ConstraintForeignKey, t.Name, cols, refTable)
		enforced := true
		t.Constraints = append(t.Constraints, &schema.Constraint{
			Name:              name,
			Type:              schema.ConstraintForeignKey,
			Columns:           cols,
			ReferencedTable:   refTable,
			ReferencedColumns: []string{refCol},
			OnDelete:          col.RefOnDelete,
			OnUpdate:          col.RefOnUpdate,
			Enforced:          &enforced,
		})
	}
}
