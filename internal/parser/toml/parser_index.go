package toml

import (
	"fmt"
	"strings"

	"smf/internal/core"
)

func convertTableIndex(ti *tomlIndex) (*core.Index, error) {
	idx := &core.Index{
		Name:    ti.Name,
		Unique:  ti.Unique,
		Comment: ti.Comment,
	}

	if ti.Type != "" {
		idx.Type = core.IndexType(ti.Type)
	} else {
		idx.Type = core.IndexTypeBTree
	}

	if ti.Visibility != "" {
		idx.Visibility = core.IndexVisibility(ti.Visibility)
	} else {
		idx.Visibility = core.IndexVisible
	}

	idx.Columns = mergeColumnIndexes(ti)

	if len(idx.Columns) == 0 {
		name := ti.Name
		if name == "" {
			name = "(unnamed)"
		}
		return nil, fmt.Errorf("index %s has no columns", name)
	}

	return idx, nil
}

func mergeColumnIndexes(ti *tomlIndex) []core.ColumnIndex {
	if len(ti.ColumnDefs) > 0 {
		cols := make([]core.ColumnIndex, 0, len(ti.ColumnDefs))
		for i := range ti.ColumnDefs {
			cols = append(cols, convertColumnIndex(&ti.ColumnDefs[i]))
		}
		return cols
	}

	if len(ti.Columns) > 0 {
		cols := make([]core.ColumnIndex, 0, len(ti.Columns))
		for _, name := range ti.Columns {
			cols = append(cols, core.ColumnIndex{
				Name:  name,
				Order: core.SortAsc,
			})
		}
		return cols
	}

	return nil
}

func convertColumnIndex(tc *tomlColumnIndex) core.ColumnIndex {
	ic := core.ColumnIndex{
		Name:   tc.Name,
		Length: tc.Length,
	}

	if tc.Order != "" {
		ic.Order = core.SortOrder(tc.Order)
	} else {
		ic.Order = core.SortAsc
	}

	return ic
}

// validateIndexes checks for duplicate names and verifies that every index
// column references an existing table column.
func validateIndexes(table *core.Table) error {
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
		for _, ic := range idx.Columns {
			if table.FindColumn(ic.Name) == nil {
				return fmt.Errorf("index %q references nonexistent column %q", idx.Name, ic.Name)
			}
		}
	}

	return nil
}
