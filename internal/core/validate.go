package core

import (
	"fmt"
	"regexp"
	"strings"
)

// ValidateDatabase runs all structural and semantic validation on a fully-built
// Database. It also synthesizes constraints from column-level shortcuts (PK,
// UNIQUE, CHECK, FK) after verifying there are no PK conflicts. This function
// should be called after the parser has finished converting input into core
// types. Returns the first error encountered.
func ValidateDatabase(db *Database) error {
	if err := validateDatabaseBasics(db); err != nil {
		return err
	}

	nameRe, err := compileAllowedNamePattern(db.Validation)
	if err != nil {
		return err
	}

	if err := validateDuplicateTableNames(db.Tables); err != nil {
		return err
	}

	if err := prevalidateAndSynthesizeTables(db.Tables); err != nil {
		return err
	}

	if err := validateAllTables(db.Tables, db.Validation, nameRe); err != nil {
		return err
	}

	return nil
}

func validateDatabaseBasics(db *Database) error {
	if db == nil {
		return fmt.Errorf("database is nil")
	}
	if db.Dialect == nil {
		return fmt.Errorf("dialect is required; supported dialects: %v", SupportedDialects())
	}
	return nil
}

func compileAllowedNamePattern(rules *ValidationRules) (*regexp.Regexp, error) {
	if rules == nil || rules.AllowedNamePattern == "" {
		return nil, nil
	}
	re, err := regexp.Compile(rules.AllowedNamePattern)
	if err != nil {
		return nil, fmt.Errorf("invalid allowed_name_pattern %q: %w", rules.AllowedNamePattern, err)
	}
	return re, nil
}

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
		return fmt.Errorf("table has no columns")
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

// validateColumn checks a single column for structural correctness.
func validateColumn(col *Column, rules *ValidationRules, nameRe *regexp.Regexp) error {
	if err := validateName(col.Name, "column", rules, nameRe, false); err != nil {
		return err
	}

	if col.References != "" {
		if _, _, ok := ParseReferences(col.References); !ok {
			return fmt.Errorf("invalid references %q: expected format \"table.column\"", col.References)
		}
	}

	return nil
}

// validateName checks that a name is non-empty and satisfies the optional
// length and pattern rules. useTableLength selects which max-length field to
// use from the validation rules.
func validateName(name, kind string, rules *ValidationRules, nameRe *regexp.Regexp, useTableLength bool) error {
	if strings.TrimSpace(name) == "" {
		return fmt.Errorf("%s name is empty", kind)
	}
	if rules == nil {
		return nil
	}

	maxLen := rules.MaxColumnNameLength
	if useTableLength {
		maxLen = rules.MaxTableNameLength
	}
	if maxLen > 0 && len(name) > maxLen {
		return fmt.Errorf("%s %q exceeds maximum length %d", kind, name, maxLen)
	}
	if nameRe != nil && !nameRe.MatchString(name) {
		return fmt.Errorf("%s %q does not match allowed pattern %q", kind, name, nameRe.String())
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
		return fmt.Errorf(
			"multiple PRIMARY KEY constraints declared; a table can have at most one primary key",
		)
	}
	if hasColumnPK && constraintPKCount > 0 {
		return fmt.Errorf(
			"primary key declared on both column(s) and in constraints section; " +
				"use column-level primary_key for single-column PKs or a constraint for composite PKs, not both",
		)
	}
	return nil
}

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

// validateIndexes checks for duplicate index names and verifies that every
// index column references an existing table column.
func validateIndexes(table *Table) error {
	seen := make(map[string]bool, len(table.Indexes))
	for _, idx := range table.Indexes {
		if idx.Name == "" {
			continue
		}
		lower := strings.ToLower(idx.Name)
		if seen[lower] {
			return fmt.Errorf("duplicate index name %q", idx.Name)
		}
		seen[lower] = true
	}

	for _, idx := range table.Indexes {
		if len(idx.Columns) == 0 {
			name := idx.Name
			if name == "" {
				name = "(unnamed)"
			}
			return fmt.Errorf("index %s has no columns", name)
		}
		for _, ic := range idx.Columns {
			if table.FindColumn(ic.Name) == nil {
				return fmt.Errorf("index %q references nonexistent column %q", idx.Name, ic.Name)
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
