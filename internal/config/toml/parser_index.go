package toml

import (
	"fmt"

	"smf/internal/schema"
)

// tomlIndex maps [[tables.indexes]].
type tomlIndex struct {
	Name       string `toml:"name"`
	Unique     bool   `toml:"unique"`
	Type       string `toml:"type"`
	Comment    string `toml:"comment"`
	Visibility string `toml:"visibility"`

	// Simple form: columns = ["tenant_id", "created_at"]
	Columns []string `toml:"columns"`

	// Advanced form: [[tables.indexes.column_defs]]
	ColumnDefs []tomlColumnIndex `toml:"column_defs"`
}

// tomlColumnIndex maps [[tables.indexes.column_defs]].
type tomlColumnIndex struct {
	Name   string `toml:"name"`
	Length int    `toml:"length"`
	Order  string `toml:"order"`
}

func index(ti *tomlIndex) (*schema.Index, error) {
	if len(ti.ColumnDefs) > 0 && len(ti.Columns) > 0 {
		return nil, fmt.Errorf("index %q: specify either columns or column_defs, not both", ti.Name)
	}

	idx := &schema.Index{
		Name:    ti.Name,
		Unique:  ti.Unique,
		Comment: ti.Comment,
	}

	if ti.Type != "" {
		idx.Type = schema.IndexType(ti.Type)
	} else {
		idx.Type = schema.IndexTypeBTree
	}

	if ti.Visibility != "" {
		idx.Visibility = schema.IndexVisibility(ti.Visibility)
	} else {
		idx.Visibility = schema.IndexVisible
	}

	idx.Columns = mergeColumnIndexes(ti)

	return idx, nil
}

func mergeColumnIndexes(ti *tomlIndex) []schema.ColumnIndex {
	if len(ti.ColumnDefs) > 0 {
		cols := make([]schema.ColumnIndex, 0, len(ti.ColumnDefs))
		for i := range ti.ColumnDefs {
			cols = append(cols, columnIndex(&ti.ColumnDefs[i]))
		}
		return cols
	}

	if len(ti.Columns) > 0 {
		cols := make([]schema.ColumnIndex, 0, len(ti.Columns))
		for _, name := range ti.Columns {
			cols = append(cols, schema.ColumnIndex{
				Name:  name,
				Order: schema.SortAsc,
			})
		}
		return cols
	}

	return nil
}

func columnIndex(tc *tomlColumnIndex) schema.ColumnIndex {
	ic := schema.ColumnIndex{
		Name:   tc.Name,
		Length: tc.Length,
	}

	if tc.Order != "" {
		ic.Order = schema.SortOrder(tc.Order)
	} else {
		ic.Order = schema.SortAsc
	}

	return ic
}
