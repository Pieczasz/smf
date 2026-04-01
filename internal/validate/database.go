// Package validate contains various validation logic e.g., for database, tables structure, dialect
// specific logic, columns. It makes sure basic schema structs are valid and we can perform, diffing,
// and migration logic.
package validate

import (
	"errors"
	"fmt"
	"regexp"
	"smf/internal/prepare"
	"strings"

	"smf/internal/schema"
)

// Database runs the full preparation-then-validation pipeline.
// It first synthesizes implicit constraints (Prepare) and then validates
// the resulting schema (Validate).
//
// TODO: consider using errors.Join to report all validation
// errors at once instead of failing on the first one.
func Database(db *schema.Database) error {
	if err := RequiredFields(db); err != nil {
		return err
	}

	if err := prepare.Database(db); err != nil {
		return err
	}
	return Validate(db)
}

// Validate checks a fully-prepared database schema for structural
// correctness without mutating it.
func Validate(db *schema.Database) error {
	nameRe, err := AllowedNamePattern(db.Validation)
	if err != nil {
		return err
	}

	if err := TableUniqueness(db.Tables); err != nil {
		return err
	}

	if err := TableStructures(db.Tables, db.Validation, nameRe); err != nil {
		return err
	}

	if err := ForeignKeys(db.Tables); err != nil {
		return err
	}

	if err := LogicalRules(db.Tables, db.Dialect); err != nil {
		return err
	}

	return Enums(db)
}

func RequiredFields(db *schema.Database) error {
	if db == nil {
		return errors.New("database is nil")
	}
	if db.Dialect == "" {
		return fmt.Errorf("dialect is required; supported dialects: %v", schema.SupportedDialects())
	}
	if !Dialect(string(db.Dialect)) {
		return fmt.Errorf("unsupported dialect %q; supported dialects: %v", db.Dialect, schema.SupportedDialects())
	}
	if strings.TrimSpace(db.Name) == "" {
		return errors.New("database name is required")
	}
	if len(db.Tables) == 0 {
		return errors.New("schema is empty, declare some tables first")
	}
	return nil
}

// Dialect reports whether d is a recognized dialect string.
func Dialect(d string) bool {
	for _, supported := range schema.SupportedDialects() {
		if strings.EqualFold(string(supported), d) {
			return true
		}
	}
	return false
}

func AllowedNamePattern(rules *schema.ValidationRules) (*regexp.Regexp, error) {
	if rules == nil || rules.AllowedNamePattern == "" {
		return nil, nil
	}
	re, err := regexp.Compile(rules.AllowedNamePattern)
	if err != nil {
		return nil, fmt.Errorf("invalid allowed_name_pattern %q: %w", rules.AllowedNamePattern, err)
	}
	return re, nil
}

var snakeCaseRe = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

func SnakeCase(s string) bool {
	return snakeCaseRe.MatchString(s)
}

func Name(name string, rules *schema.ValidationRules, nameRe *regexp.Regexp, useTableLength bool) error {
	if strings.TrimSpace(name) == "" {
		return errors.New("name is empty")
	}

	if !SnakeCase(name) {
		return fmt.Errorf("%q must be in snake_case", name)
	}

	if rules == nil {
		return nil
	}

	maxLen := rules.MaxColumnNameLength
	if useTableLength {
		maxLen = rules.MaxTableNameLength
	}
	if maxLen > 0 && len(name) > maxLen {
		return fmt.Errorf("%q exceeds maximum length %d", name, maxLen)
	}
	if nameRe != nil && !nameRe.MatchString(name) {
		return fmt.Errorf("%q does not match allowed pattern %q", name, nameRe.String())
	}
	return nil
}
