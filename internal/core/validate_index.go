package core

import (
	"fmt"
	"strings"
)

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
