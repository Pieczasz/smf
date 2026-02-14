package toml

import (
	"smf/internal/core"
)

// tomlIndex maps [[tables.indexes]].
type tomlIndex struct {
	Name       string `toml:"name"`
	Unique     bool   `toml:"unique"`
	Type       string `toml:"type"`
	Comment    string `toml:"comment"`
	Visibility string `toml:"visibility"`

	// Simple form:  columns = ["tenant_id", "created_at"]
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

func convertTableIndex(ti *tomlIndex) *core.Index {
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

	return idx
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
