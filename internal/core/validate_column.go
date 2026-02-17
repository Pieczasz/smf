package core

import (
	"fmt"
	"regexp"
)

// Validate checks a single column for structural correctness.
func (c *Column) Validate(rules *ValidationRules, nameRe *regexp.Regexp) error {
	if err := validateName(c.Name, rules, nameRe, false); err != nil {
		return fmt.Errorf("column %w", err)
	}

	if c.Type == "" && c.RawType == "" || c.Type == DataTypeUnknown {
		return fmt.Errorf("column %q: type is empty", c.Name)
	}

	// TODO: validate this field (col.DefaultValue)
	// TODO: validate this field (col.OnUpdate)
	// TODO: validate this field (col.Comment)
	// TODO: validate this field (col.Collate)
	// TODO: validate this field (col.Charset)
	// TODO: validate this field (col.EnumValues)
	// TODO: validate this field (col.IdentitySeed)
	// TODO: validate this field (col.IdentityIncrement)
	// TODO: validate this field (col.SequenceName)
	if err := c.validateOptions(); err != nil {
		return fmt.Errorf("column %q: %w", c.Name, err)
	}

	if c.References != "" {
		if _, _, ok := ParseReferences(c.References); !ok {
			return fmt.Errorf("column %q: invalid references %q: expected format \"table.column\"", c.Name, c.References)
		}
	}

	return nil
}

func (c *Column) validateOptions() error {
	return nil
}
