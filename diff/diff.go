package diff

import (
	"fmt"
	"os"
	"schemift/core"
	"sort"
	"strconv"
	"strings"
)

const (
	renameDetectionScoreThreshold = 9
	renameSharedTokenMinLen       = 3
)

type SchemaDiff struct {
	AddedTables    []*core.Table
	RemovedTables  []*core.Table
	ModifiedTables []*TableDiff
}

type TableDiff struct {
	Name                string
	AddedColumns        []*core.Column
	RemovedColumns      []*core.Column
	RenamedColumns      []*ColumnRename
	ModifiedColumns     []*ColumnChange
	AddedConstraints    []*core.Constraint
	RemovedConstraints  []*core.Constraint
	ModifiedConstraints []*ConstraintChange
	AddedIndexes        []*core.Index
	RemovedIndexes      []*core.Index
	ModifiedIndexes     []*IndexChange
	ModifiedOptions     []*TableOptionChange
}

type ColumnChange struct {
	Name    string
	Old     *core.Column
	New     *core.Column
	Changes []*FieldChange
}

type ColumnRename struct {
	Old   *core.Column
	New   *core.Column
	Score int
}

type ConstraintChange struct {
	Name          string
	Old           *core.Constraint
	New           *core.Constraint
	Changes       []*FieldChange
	RebuildOnly   bool
	RebuildReason string
}

type IndexChange struct {
	Name    string
	Old     *core.Index
	New     *core.Index
	Changes []*FieldChange
}

type FieldChange struct {
	Field string
	Old   string
	New   string
}

type TableOptionChange struct {
	Name string
	Old  string
	New  string
}

func Diff(oldDB, newDB *core.Database) *SchemaDiff {
	d := &SchemaDiff{}

	oldTables := mapByLowerName(oldDB.Tables, func(t *core.Table) string { return t.Name })
	newTables := mapByLowerName(newDB.Tables, func(t *core.Table) string { return t.Name })

	for name, nt := range newTables {
		ot, ok := oldTables[name]
		if !ok {
			d.AddedTables = append(d.AddedTables, nt)
			continue
		}

		td := compareTable(ot, nt)
		if td != nil {
			d.ModifiedTables = append(d.ModifiedTables, td)
		}
	}

	for name, ot := range oldTables {
		if _, ok := newTables[name]; !ok {
			d.RemovedTables = append(d.RemovedTables, ot)
		}
	}

	sortByNameCI(d.AddedTables, func(t *core.Table) string { return t.Name })
	sortByNameCI(d.RemovedTables, func(t *core.Table) string { return t.Name })
	sortByNameCI(d.ModifiedTables, func(td *TableDiff) string { return td.Name })

	return d
}

func (c *fieldChangeCollector) Add(field, oldV, newV string) {
	if oldV == newV {
		return
	}
	c.Changes = append(c.Changes, &FieldChange{Field: field, Old: oldV, New: newV})
}

func (d *SchemaDiff) String() string {
	if d.IsEmpty() {
		return "No differences detected."
	}

	var sb strings.Builder
	sb.WriteString("Schema differences:\n")

	if len(d.AddedTables) > 0 {
		sb.WriteString("\nAdded tables:\n")
		for _, t := range d.AddedTables {
			sb.WriteString(fmt.Sprintf("  - %s\n", t.Name))
		}
	}

	if len(d.RemovedTables) > 0 {
		sb.WriteString("\nRemoved tables:\n")
		for _, t := range d.RemovedTables {
			sb.WriteString(fmt.Sprintf("  - %s\n", t.Name))
		}
	}

	if len(d.ModifiedTables) > 0 {
		sb.WriteString("\nModified tables:\n")
		for _, mt := range d.ModifiedTables {
			d.writeTableDiff(&sb, mt)
		}
	}

	return sb.String()
}

func equalStringSliceCI(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !strings.EqualFold(a[i], b[i]) {
			return false
		}
	}
	return true
}

func u64(v uint64) string {
	return strconv.FormatUint(v, 10)
}

type fieldChangeCollector struct {
	Changes []*FieldChange
}

func sortByNameCI[T any](items []T, name func(T) string) {
	sort.Slice(items, func(i, j int) bool {
		return strings.ToLower(name(items[i])) < strings.ToLower(name(items[j]))
	})
}

func mapByLowerName[T any](items []T, name func(T) string) map[string]T {
	m := make(map[string]T, len(items))
	for _, item := range items {
		m[strings.ToLower(name(item))] = item
	}
	return m
}

func mapByKey[T any](items []T, key func(T) string) map[string]T {
	m := make(map[string]T, len(items))
	for _, item := range items {
		m[key(item)] = item
	}
	return m
}

func ptrStr(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

func formatNameList(items []string) string {
	return "(" + strings.Join(items, ", ") + ")"
}

func (d *SchemaDiff) IsEmpty() bool {
	return len(d.AddedTables) == 0 && len(d.RemovedTables) == 0 && len(d.ModifiedTables) == 0
}

func (d *SchemaDiff) SaveToFile(path string) error {
	return os.WriteFile(path, []byte(d.String()), 0644)
}
