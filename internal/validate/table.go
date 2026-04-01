package validate

import (
	"errors"
	"fmt"
	"regexp"

	"smf/internal/schema"
)

func TableUniqueness(tables []*schema.Table) error {
	seenTables := make(map[string]bool, len(tables))
	for _, table := range tables {
		if seenTables[table.Name] {
			return fmt.Errorf("duplicate table name %q", table.Name)
		}
		seenTables[table.Name] = true
	}
	return nil
}


func TableStructures(tables []*schema.Table, rules *schema.ValidationRules, nameRe *regexp.Regexp) error {
	for _, table := range tables {
		if err := Table(table, rules, nameRe); err != nil {
			return err
		}
	}
	return nil
}

func Table(t *schema.Table, rules *schema.ValidationRules, nameRe *regexp.Regexp) error {
	// TODO: options part
	if err := TableNameAndOptions(t, rules, nameRe); err != nil {
		return err
	}
	if err := Columns(t, rules, nameRe); err != nil {
		return err
	}
	if err := Constraints(t); err != nil {
		return err
	}
	if err := Timestamps(t); err != nil {
		return err
	}
	return Indexes(t)
}

func TableNameAndOptions(t *schema.Table, rules *schema.ValidationRules, nameRe *regexp.Regexp) error {
	if err := Name(t.Name, rules, nameRe, true); err != nil {
		return fmt.Errorf("table %q: %w", t.Name, err)
	}
	if err := TableOptions(&t.Options); err != nil {
		return fmt.Errorf("table %q: %w", t.Name, err)
	}
	return nil
}

func Columns(t *schema.Table, rules *schema.ValidationRules, nameRe *regexp.Regexp) error {
	if len(t.Columns) == 0 {
		return fmt.Errorf("table %q has no columns", t.Name)
	}
	seenCols := make(map[string]bool, len(t.Columns))
	for _, col := range t.Columns {
		if seenCols[col.Name] {
			return fmt.Errorf("table %q: duplicate column name %q", t.Name, col.Name)
		}
		seenCols[col.Name] = true
	}
	for _, col := range t.Columns {
		if err := Column(col, rules, nameRe); err != nil {
			return fmt.Errorf("table %q: %w", t.Name, err)
		}
	}
	return nil
}

// TODO: implement this.
func TableOptions(_ *schema.TableOptions) error {
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

func Timestamps(t *schema.Table) error {
	if t.Timestamps == nil || !t.Timestamps.Enabled {
		return nil
	}
	createdCol := schema.DefaultCreatedColumn
	updatedCol := schema.DefaultUpdatedColumn
	if t.Timestamps.CreatedColumn != "" {
		if err := Name(t.Timestamps.CreatedColumn, nil, nil, false); err != nil {
			return fmt.Errorf("timestamp created_column: %w", err)
		}
		createdCol = t.Timestamps.CreatedColumn
	}
	if t.Timestamps.UpdatedColumn != "" {
		if err := Name(t.Timestamps.UpdatedColumn, nil, nil, false); err != nil {
			return fmt.Errorf("timestamp updated_column: %w", err)
		}
		updatedCol = t.Timestamps.UpdatedColumn
	}
	if createdCol == updatedCol {
		return fmt.Errorf("timestamps created_column and updated_column resolve to the same name %q", createdCol)
	}
	return nil
}
