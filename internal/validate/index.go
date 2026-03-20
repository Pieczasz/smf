package validate

import (
	"fmt"

	"smf/internal/schema"
)

func Indexes(t *schema.Table) error {
	if err := IndexNames(t); err != nil {
		return err
	}
	return IndexColumns(t)
}

func IndexNames(t *schema.Table) error {
	seen := make(map[string]bool, len(t.Indexes))
	for _, idx := range t.Indexes {
		if idx.Name == "" {
			continue
		}
		if err := Name(idx.Name, nil, nil, false); err != nil {
			return fmt.Errorf("index %q: %w", idx.Name, err)
		}
		if seen[idx.Name] {
			return fmt.Errorf("duplicate index name %q", idx.Name)
		}
		seen[idx.Name] = true
	}
	return nil
}

func IndexColumns(t *schema.Table) error {
	for _, idx := range t.Indexes {
		if len(idx.Columns) == 0 {
			name := idx.Name
			if name == "" {
				name = "(unnamed)"
			}
			return fmt.Errorf("index %s has no columns", name)
		}
		for _, ic := range idx.Columns {
			if t.FindColumn(ic.Name) == nil {
				return fmt.Errorf("index %q references nonexistent column %q", idx.Name, ic.Name)
			}
		}
	}
	return nil
}
