package core

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

func validateDuplicateTableNames(tables []*Table) error {
	seenTables := make(map[string]bool, len(tables))
	for _, table := range tables {
		lower := strings.ToLower(table.Name)
		if seenTables[lower] {
			return fmt.Errorf("duplicate table name %q", table.Name)
		}
		seenTables[lower] = true
	}
	return nil
}

func prevalidateAndSynthesizeTables(tables []*Table) error {
	for _, table := range tables {
		if err := validatePKConflict(table); err != nil {
			return fmt.Errorf("table %q: %w", table.Name, err)
		}
		synthesizeConstraints(table)
	}
	return nil
}

func validateAllTables(tables []*Table, rules *ValidationRules, nameRe *regexp.Regexp) error {
	for _, table := range tables {
		if err := validateTable(table, rules, nameRe); err != nil {
			return fmt.Errorf("table %q: %w", table.Name, err)
		}
	}
	return nil
}

// validateTable checks a single table for structural correctness.
func validateTable(table *Table, rules *ValidationRules, nameRe *regexp.Regexp) error {
	if err := validateName(table.Name, "table", rules, nameRe, true); err != nil {
		return err
	}

	if len(table.Columns) == 0 {
		return errors.New("table has no columns")
	}

	seenCols := make(map[string]bool, len(table.Columns))
	for _, col := range table.Columns {
		lower := strings.ToLower(col.Name)
		if seenCols[lower] {
			return fmt.Errorf("duplicate column name %q", col.Name)
		}
		seenCols[lower] = true
	}

	for _, col := range table.Columns {
		if err := validateColumn(col, rules, nameRe); err != nil {
			return fmt.Errorf("column %q: %w", col.Name, err)
		}
	}

	if err := validateConstraints(table); err != nil {
		return err
	}

	if err := validateTimestamps(table); err != nil {
		return err
	}

	if err := validateIndexes(table); err != nil {
		return err
	}

	return nil
}

// validatePKConflict ensures a table doesn't define primary keys both at the
// column level (primary_key = true) and in the constraints section. This check
// MUST run before synthesizeConstraints because synthesis merges column-level
// PKs into constraint-level, making the conflict undetectable.
func validatePKConflict(table *Table) error {
	hasColumnPK := false
	for _, col := range table.Columns {
		if col.PrimaryKey {
			hasColumnPK = true
			break
		}
	}
	constraintPKCount := 0
	for _, con := range table.Constraints {
		if con.Type == ConstraintPrimaryKey {
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

// validateTimestamps checks that the created and updated timestamp columns
// resolve to distinct names.
func validateTimestamps(table *Table) error {
	if table.Timestamps == nil || !table.Timestamps.Enabled {
		return nil
	}
	createdCol := "created_at"
	updatedCol := "updated_at"
	if table.Timestamps.CreatedColumn != "" {
		createdCol = table.Timestamps.CreatedColumn
	}
	if table.Timestamps.UpdatedColumn != "" {
		updatedCol = table.Timestamps.UpdatedColumn
	}
	if strings.EqualFold(createdCol, updatedCol) {
		return fmt.Errorf("timestamps created_column and updated_column resolve to the same name %q", createdCol)
	}
	return nil
}
