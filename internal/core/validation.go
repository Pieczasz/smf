package core

import (
	"fmt"
	"regexp"
	"strings"
)

// ValidationError represents an error during schema validation.
type ValidationError struct {
	Entity  string
	Name    string
	Field   string
	Message string
}

func (e *ValidationError) Error() string {
	if e.Field != "" {
		return fmt.Sprintf("validation error in %s %q field %q: %s", e.Entity, e.Name, e.Field, e.Message)
	}
	return fmt.Sprintf("validation error in %s %q: %s", e.Entity, e.Name, e.Message)
}

// Validate checks if the Database schema is valid and returns an error if not.
func (db *Database) Validate() error {
	if db == nil {
		return &ValidationError{Entity: "database", Message: "database is nil"}
	}

	var nameRe *regexp.Regexp
	if db.Validation != nil && db.Validation.AllowedNamePattern != "" {
		var err error
		nameRe, err = regexp.Compile(db.Validation.AllowedNamePattern)
		if err != nil {
			return &ValidationError{
				Entity:  "database",
				Name:    db.Name,
				Field:   "Validation.AllowedNamePattern",
				Message: fmt.Sprintf("invalid regex: %v", err),
			}
		}
	}

	seen := make(map[string]bool)
	for i, t := range db.Tables {
		if t == nil {
			return &ValidationError{Entity: "database", Name: db.Name, Message: fmt.Sprintf("table at index %d is nil", i)}
		}
		nameLower := strings.ToLower(t.Name)
		if seen[nameLower] {
			return &ValidationError{Entity: "database", Name: db.Name, Message: fmt.Sprintf("duplicate table name %q", t.Name)}
		}
		seen[nameLower] = true

		if err := t.Validate(); err != nil {
			return err
		}
		if err := applyNamingRules(t, db.Validation, nameRe); err != nil {
			return err
		}
	}
	return nil
}

func applyNamingRules(t *Table, rules *ValidationRules, nameRe *regexp.Regexp) error {
	if rules == nil {
		return nil
	}

	if err := checkIdentifier("table", t.Name, "", rules.MaxTableNameLength, nameRe); err != nil {
		return err
	}
	for _, c := range t.Columns {
		if err := checkIdentifier("column", c.Name, "", rules.MaxColumnNameLength, nameRe); err != nil {
			return err
		}
	}
	return nil
}

// Validate checks if the Table definition is valid and returns an error if not.
func (t *Table) Validate() error {
	if t == nil {
		return &ValidationError{Entity: "table", Message: "table is nil"}
	}
	if strings.TrimSpace(t.Name) == "" {
		return &ValidationError{Entity: "table", Name: "(empty)", Message: "table name is empty"}
	}
	if len(t.Columns) == 0 {
		return &ValidationError{Entity: "table", Name: t.Name, Message: "table has no columns"}
	}

	if err := validateTableColumns(t); err != nil {
		return err
	}
	if err := validateNoDuplicatePrimaryKey(t); err != nil {
		return err
	}
	if err := validateTableConstraints(t); err != nil {
		return err
	}
	if err := validateTableIndexes(t); err != nil {
		return err
	}
	if err := validateTimestampsConfig(t); err != nil {
		return err
	}

	return nil
}

func validateTableColumns(t *Table) error {
	seenCols := make(map[string]bool)
	for i, c := range t.Columns {
		if c == nil {
			return &ValidationError{Entity: "table", Name: t.Name, Message: fmt.Sprintf("column at index %d is nil", i)}
		}
		if err := c.Validate(); err != nil {
			return err
		}
		nameLower := strings.ToLower(c.Name)
		if seenCols[nameLower] {
			return &ValidationError{Entity: "table", Name: t.Name, Message: fmt.Sprintf("duplicate column name %q", c.Name)}
		}
		seenCols[nameLower] = true
	}
	return nil
}

// Validate checks if the Column definition is valid and returns an error if not.
func (c *Column) Validate() error {
	if c == nil {
		return &ValidationError{Entity: "column", Message: "column is nil"}
	}
	if strings.TrimSpace(c.Name) == "" {
		return &ValidationError{Entity: "column", Name: "(empty)", Message: "column name is empty"}
	}
	if strings.TrimSpace(c.TypeRaw) == "" {
		return &ValidationError{Entity: "column", Name: c.Name, Field: "TypeRaw", Message: "column type is empty"}
	}

	if err := c.validateGenerated(); err != nil {
		return err
	}
	if err := c.validateReferences(); err != nil {
		return err
	}
	if err := c.validateEnumValues(); err != nil {
		return err
	}

	return nil
}

func (c *Column) validateGenerated() error {
	if c.IsGenerated && strings.TrimSpace(c.GenerationExpression) == "" {
		return &ValidationError{Entity: "column", Name: c.Name, Field: "GenerationExpression", Message: "generated column must have an expression"}
	}
	return nil
}

func (c *Column) validateReferences() error {
	if c.References != "" {
		if _, _, ok := ParseReferences(c.References); !ok {
			return &ValidationError{
				Entity:  "column",
				Name:    c.Name,
				Field:   "References",
				Message: fmt.Sprintf("invalid reference format %q; expected \"table.column\"", c.References),
			}
		}
	}
	return nil
}

func (c *Column) validateEnumValues() error {
	if c.Type == DataTypeEnum && len(c.EnumValues) == 0 {
		if !strings.Contains(strings.ToLower(c.TypeRaw), "enum(") || !strings.Contains(c.TypeRaw, "'") {
			return &ValidationError{
				Entity:  "column",
				Name:    c.Name,
				Field:   "EnumValues",
				Message: "enum column must have values (use values = [\"a\", \"b\"])",
			}
		}
	}
	return nil
}

func validateNoDuplicatePrimaryKey(t *Table) error {
	pkCount := 0
	for _, c := range t.Constraints {
		if c != nil && c.Type == ConstraintPrimaryKey {
			pkCount++
		}
	}
	if pkCount > 1 {
		return &ValidationError{
			Entity:  "table",
			Name:    t.Name,
			Message: "table has multiple PRIMARY KEY constraints; only one is allowed",
		}
	}
	return nil
}

func validateTableConstraints(t *Table) error {
	seenConstr := make(map[string]bool)
	for i, c := range t.Constraints {
		if c == nil {
			return &ValidationError{Entity: "table", Name: t.Name, Message: fmt.Sprintf("constraint at index %d is nil", i)}
		}
		if err := c.Validate(); err != nil {
			return err
		}
		if c.Name == "" {
			continue
		}
		nameLower := strings.ToLower(c.Name)
		if seenConstr[nameLower] {
			return &ValidationError{Entity: "table", Name: t.Name, Message: fmt.Sprintf("duplicate constraint name %q", c.Name)}
		}
		seenConstr[nameLower] = true
	}
	return nil
}

// Validate checks if the Constraint definition is valid and returns an error if not.
func (c *Constraint) Validate() error {
	if c == nil {
		return &ValidationError{Entity: "constraint", Message: "constraint is nil"}
	}
	if c.Type != ConstraintCheck && len(c.Columns) == 0 {
		return &ValidationError{Entity: "constraint", Name: c.Name, Field: "Columns", Message: "constraint has no columns"}
	}
	if c.Type == ConstraintForeignKey {
		if strings.TrimSpace(c.ReferencedTable) == "" {
			return &ValidationError{Entity: "constraint", Name: c.Name, Field: "ReferencedTable", Message: "foreign key must reference a table"}
		}
		if len(c.ReferencedColumns) == 0 {
			return &ValidationError{Entity: "constraint", Name: c.Name, Field: "ReferencedColumns", Message: "foreign key must reference columns"}
		}
		if len(c.Columns) != len(c.ReferencedColumns) {
			return &ValidationError{Entity: "constraint", Name: c.Name, Message: "foreign key column count mismatch"}
		}
	}
	if c.Type == ConstraintCheck && strings.TrimSpace(c.CheckExpression) == "" {
		return &ValidationError{Entity: "constraint", Name: c.Name, Field: "CheckExpression", Message: "check constraint must have an expression"}
	}
	return nil
}

func validateTableIndexes(t *Table) error {
	seenIdx := make(map[string]bool)
	for i, idx := range t.Indexes {
		if idx == nil {
			return &ValidationError{Entity: "table", Name: t.Name, Message: fmt.Sprintf("index at index %d is nil", i)}
		}
		if err := idx.Validate(); err != nil {
			return err
		}
		if idx.Name == "" {
			continue
		}
		nameLower := strings.ToLower(idx.Name)
		if seenIdx[nameLower] {
			return &ValidationError{Entity: "table", Name: t.Name, Message: fmt.Sprintf("duplicate index name %q", idx.Name)}
		}
		seenIdx[nameLower] = true
	}
	return nil
}

// Validate checks if the Index definition is valid and returns an error if not.
func (i *Index) Validate() error {
	if i == nil {
		return &ValidationError{Entity: "index", Message: "index is nil"}
	}
	if len(i.Columns) == 0 {
		return &ValidationError{Entity: "index", Name: i.Name, Field: "Columns", Message: "index has no columns"}
	}
	for j, col := range i.Columns {
		if strings.TrimSpace(col.Name) == "" {
			return &ValidationError{Entity: "index", Name: i.Name, Message: fmt.Sprintf("index column at position %d has empty name", j)}
		}
	}
	return nil
}

func validateTimestampsConfig(t *Table) error {
	if t.Timestamps == nil || !t.Timestamps.Enabled {
		return nil
	}
	createdCol := t.Timestamps.CreatedColumn
	if createdCol == "" {
		createdCol = "created_at"
	}
	updatedCol := t.Timestamps.UpdatedColumn
	if updatedCol == "" {
		updatedCol = "updated_at"
	}
	if strings.EqualFold(createdCol, updatedCol) {
		return &ValidationError{
			Entity:  "table",
			Name:    t.Name,
			Field:   "Timestamps",
			Message: "created and updated column names must differ",
		}
	}
	return nil
}

func checkIdentifier(entity, name, field string, maxLen int, nameRe *regexp.Regexp) error {
	if maxLen > 0 && len(name) > maxLen {
		return &ValidationError{
			Entity:  entity,
			Name:    name,
			Field:   field,
			Message: fmt.Sprintf("name exceeds maximum length of %d characters", maxLen),
		}
	}
	if nameRe != nil && !nameRe.MatchString(name) {
		return &ValidationError{
			Entity:  entity,
			Name:    name,
			Field:   field,
			Message: fmt.Sprintf("name does not match allowed pattern %q", nameRe.String()),
		}
	}
	return nil
}

// AutoGenerateConstraintName produces a deterministic name for a constraint
// that was synthesized from column-level shortcuts.
//
//	PK:     pk_{table}
//	UNIQUE: uq_{table}_{column}
//	CHECK:  chk_{table}_{column}
//	FK:     fk_{table}_{referenced_table}
func AutoGenerateConstraintName(ctype ConstraintType, table string, columns []string, refTable string) string {
	t := strings.ToLower(table)
	switch ctype {
	case ConstraintPrimaryKey:
		return fmt.Sprintf("pk_%s", t)
	case ConstraintUnique:
		return fmt.Sprintf("uq_%s_%s", t, strings.ToLower(strings.Join(columns, "_")))
	case ConstraintCheck:
		return fmt.Sprintf("chk_%s_%s", t, strings.ToLower(strings.Join(columns, "_")))
	case ConstraintForeignKey:
		return fmt.Sprintf("fk_%s_%s", t, strings.ToLower(refTable))
	default:
		return fmt.Sprintf("cstr_%s_%s", t, strings.ToLower(strings.Join(columns, "_")))
	}
}
