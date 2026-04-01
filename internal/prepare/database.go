package prepare

import (
	"errors"
	"fmt"

	"smf/internal/schema"
)

// Database runs pre-validation transformations that mutate the schema.
// Currently this synthesizes implicit constraints from column-level
// shorthand (primary_key, unique, check, references).
func Database(db *schema.Database) error {
	return SynthesizeConstraints(db.Tables)
}

func SynthesizeConstraints(tables []*schema.Table) error {
	for _, table := range tables {
		if err := PrimaryKeyConflict(table); err != nil {
			return fmt.Errorf("table %q: %w", table.Name, err)
		}
		Synthesize(table)
	}
	return nil
}

func PrimaryKeyConflict(t *schema.Table) error {
	hasColumnPK := false
	for _, col := range t.Columns {
		if col.PrimaryKey {
			hasColumnPK = true
			break
		}
	}
	constraintPKCount := 0
	for _, con := range t.Constraints {
		if con.Type == schema.ConstraintPrimaryKey {
			constraintPKCount++
		}
	}
	if constraintPKCount > 1 {
		return errors.New("multiple PRIMARY KEY constraints declared; a table can have at most one primary key")
	}
	if hasColumnPK && constraintPKCount > 0 {
		return errors.New("primary key declared on both column(s) and in constraints section")
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
		t.Constraints = append(t.Constraints, &schema.Constraint{
			Name:            name,
			Type:            schema.ConstraintCheck,
			CheckExpression: col.Check,
			Enforced:        new(true),
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
		t.Constraints = append(t.Constraints, &schema.Constraint{
			Name:              name,
			Type:              schema.ConstraintForeignKey,
			Columns:           cols,
			ReferencedTable:   refTable,
			ReferencedColumns: []string{refCol},
			OnDelete:          col.RefOnDelete,
			OnUpdate:          col.RefOnUpdate,
			Enforced:          new(true),
		})
	}
}
