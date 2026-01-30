package core

import (
	"fmt"
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

	// Check for duplicate column names
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

	// Check for duplicate constraint names (except empty names)
	seenConstr := make(map[string]bool)
	for i, c := range t.Constraints {
		if c == nil {
			return &ValidationError{Entity: "table", Name: t.Name, Message: fmt.Sprintf("constraint at index %d is nil", i)}
		}
		if err := c.Validate(); err != nil {
			return err
		}
		if c.Name != "" {
			nameLower := strings.ToLower(c.Name)
			if seenConstr[nameLower] {
				return &ValidationError{Entity: "table", Name: t.Name, Message: fmt.Sprintf("duplicate constraint name %q", c.Name)}
			}
			seenConstr[nameLower] = true
		}
	}

	// Check for duplicate index names (except empty names)
	seenIdx := make(map[string]bool)
	for i, idx := range t.Indexes {
		if idx == nil {
			return &ValidationError{Entity: "table", Name: t.Name, Message: fmt.Sprintf("index at index %d is nil", i)}
		}
		if err := idx.Validate(); err != nil {
			return err
		}
		if idx.Name != "" {
			nameLower := strings.ToLower(idx.Name)
			if seenIdx[nameLower] {
				return &ValidationError{Entity: "table", Name: t.Name, Message: fmt.Sprintf("duplicate index name %q", idx.Name)}
			}
			seenIdx[nameLower] = true
		}
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
	if c.IsGenerated && strings.TrimSpace(c.GenerationExpression) == "" {
		return &ValidationError{Entity: "column", Name: c.Name, Field: "GenerationExpression", Message: "generated column must have an expression"}
	}
	return nil
}

// Validate checks if the Constraint definition is valid and returns an error if not.
func (c *Constraint) Validate() error {
	if c == nil {
		return &ValidationError{Entity: "constraint", Message: "constraint is nil"}
	}
	// CHECK constraints don't require columns (they use expressions)
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
