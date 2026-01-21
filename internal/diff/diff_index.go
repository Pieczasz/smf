package diff

import (
	"strconv"
	"strings"

	"smf/internal/core"
)

func compareIndexes(oldItems, newItems []*core.Index, td *TableDiff) {
	oldMap := mapByKey(oldItems, indexKey)
	newMap := mapByKey(newItems, indexKey)

	for name, newItem := range newMap {
		oldItem, exists := oldMap[name]
		if !exists {
			td.AddedIndexes = append(td.AddedIndexes, newItem)
			continue
		}
		if !equalIndex(oldItem, newItem) {
			td.ModifiedIndexes = append(td.ModifiedIndexes, &IndexChange{
				Name:    newItem.Name,
				Old:     oldItem,
				New:     newItem,
				Changes: indexFieldChanges(oldItem, newItem),
			})
		}
	}

	for name, oldItem := range oldMap {
		if _, exists := newMap[name]; !exists {
			td.RemovedIndexes = append(td.RemovedIndexes, oldItem)
		}
	}
}

func equalIndex(a, b *core.Index) bool {
	if a.Unique != b.Unique {
		return false
	}
	if a.Type != b.Type {
		return false
	}
	if !equalIndexColumns(a.Columns, b.Columns) {
		return false
	}
	if a.Comment != b.Comment {
		return false
	}
	if a.Visibility != b.Visibility {
		return false
	}
	return true
}

func equalIndexColumns(a, b []core.IndexColumn) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !strings.EqualFold(a[i].Name, b[i].Name) {
			return false
		}
		if a[i].Length != b[i].Length {
			return false
		}
		if a[i].Order != b[i].Order {
			return false
		}
	}
	return true
}

func indexFieldChanges(oldI, newI *core.Index) []*FieldChange {
	c := &fieldChangeCollector{}

	c.Add("unique", strconv.FormatBool(oldI.Unique), strconv.FormatBool(newI.Unique))
	c.Add("type", string(oldI.Type), string(newI.Type))
	c.Add("columns", formatIndexColumns(oldI.Columns), formatIndexColumns(newI.Columns))
	c.Add("comment", oldI.Comment, newI.Comment)
	c.Add("visibility", string(oldI.Visibility), string(newI.Visibility))

	return c.Changes
}

func formatIndexColumns(cols []core.IndexColumn) string {
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = c.Name
	}
	return "(" + strings.Join(names, ", ") + ")"
}

func indexKey(i *core.Index) string {
	name := strings.ToLower(strings.TrimSpace(i.Name))
	if name != "" {
		return name
	}
	uniq := "0"
	if i.Unique {
		uniq = "1"
	}
	cols := make([]string, len(i.Columns))
	for idx, c := range i.Columns {
		cols[idx] = strings.ToLower(c.Name)
	}
	return "idx:" + uniq + ":" + strings.ToLower(string(i.Type)) + ":" + strings.Join(cols, ",")
}
