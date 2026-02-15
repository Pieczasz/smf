package core

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// ValidateDatabase runs all structural and semantic validation on a fully built
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

	tableMap := make(map[string]*Table, len(db.Tables))
	for _, t := range db.Tables {
		tableMap[strings.ToLower(t.Name)] = t
	}

	if err := prevalidateAndSynthesizeTables(db.Tables); err != nil {
		return err
	}

	if err := validateAllTables(db.Tables, db.Validation, nameRe); err != nil {
		return err
	}

	if err := validateFKColumnExistence(db, tableMap); err != nil {
		return err
	}

	if err := validateSemantic(db, tableMap); err != nil {
		return err
	}

	if err := validateEnums(db); err != nil {
		return err
	}

	return nil
}

func validateDatabaseBasics(db *Database) error {
	if db == nil {
		return errors.New("database is nil")
	}
	if db.Dialect == nil {
		return fmt.Errorf("dialect is required; supported dialects: %v", SupportedDialects())
	}
	if strings.TrimSpace(db.Name) == "" {
		return errors.New("database name is required")
	}
	if len(db.Tables) == 0 {
		return errors.New("schema is empty, declare some tables first")
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
