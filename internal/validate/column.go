package validate

import (
	"fmt"
	"regexp"

	"smf/internal/schema"
)

func Column(c *schema.Column, rules *schema.ValidationRules, nameRe *regexp.Regexp) error {
	if err := Name(c.Name, rules, nameRe, false); err != nil {
		return fmt.Errorf("column %q: %w", c.Name, err)
	}

	if (c.Type == "" && c.RawType == "") || c.Type == schema.DataTypeUnknown {
		return fmt.Errorf("column %q: type is empty", c.Name)
	}

	// TODO: create this validation
	if err := ColumnOptions(c); err != nil {
		return fmt.Errorf("column %q: %w", c.Name, err)
	}

	if c.References != "" {
		if _, _, ok := schema.ParseReferences(c.References); !ok {
			return fmt.Errorf("column %q: invalid references %q: expected format \"table.column\"", c.Name, c.References)
		}
	}

	return nil
}

// TODO: implement this.
func ColumnOptions(_ *schema.Column) error {
	return nil
}
