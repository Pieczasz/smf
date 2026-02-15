package core

import (
	"fmt"
	"regexp"
)

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
