package core

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// Validate runs all structural and semantic validation on a fully built
// Database. It also synthesizes constraints from column-level shortcuts (PK,
// UNIQUE, CHECK, FK) after verifying there are no PK conflicts.
//
// This method should be called after the parser has finished converting
// input into core types. It returns the first error encountered.
func (db *Database) Validate() error {
	if err := db.validateRequiredFields(); err != nil {
		return err
	}

	nameRe, err := db.compileAllowedNamePattern()
	if err != nil {
		return err
	}

	if err := db.validateTableUniqueness(); err != nil {
		return err
	}

	if err := db.validateAndSynthesizeConstraints(); err != nil {
		return err
	}

	if err := db.validateTableStructures(nameRe); err != nil {
		return err
	}

	if err := db.validateForeignKeys(); err != nil {
		return err
	}

	if err := db.validateLogicalRules(); err != nil {
		return err
	}

	if err := db.validateEnums(); err != nil {
		return err
	}

	return nil
}

// validateRequiredFields checks for the presence of required top-level database fields.
func (db *Database) validateRequiredFields() error {
	if db == nil {
		return errors.New("database is nil")
	}
	if db.Dialect == nil {
		return fmt.Errorf("dialect is required; supported dialects: %v", SupportedDialects())
	}
	if !ValidDialect(string(*db.Dialect)) {
		return fmt.Errorf("unsupported dialect %q; supported dialects: %v", *db.Dialect, SupportedDialects())
	}
	if strings.TrimSpace(db.Name) == "" {
		return errors.New("database name is required")
	}
	if len(db.Tables) == 0 {
		return errors.New("schema is empty, declare some tables first")
	}
	return nil
}

// compileAllowedNamePattern prepares the regular expression for name validation
// if a pattern is defined in the validation rules.
func (db *Database) compileAllowedNamePattern() (*regexp.Regexp, error) {
	if db.Validation == nil || db.Validation.AllowedNamePattern == "" {
		return nil, nil
	}
	re, err := regexp.Compile(db.Validation.AllowedNamePattern)
	if err != nil {
		return nil, fmt.Errorf("invalid allowed_name_pattern %q: %w", db.Validation.AllowedNamePattern, err)
	}
	return re, nil
}

var snakeCaseRe = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

// snakeCase reports whether a string follows the snake_case naming convention.
func snakeCase(s string) bool {
	return snakeCaseRe.MatchString(s)
}

// validateName checks that a name is non-empty, satisfies the optional
// length and pattern rules, and follows snake_case convention.
// useTableLength selects which max-length field to use from the validation rules.
func validateName(name string, rules *ValidationRules, nameRe *regexp.Regexp, useTableLength bool) error {
	if strings.TrimSpace(name) == "" {
		return errors.New("name is empty")
	}

	if !snakeCase(name) {
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
