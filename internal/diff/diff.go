package diff

import (
	"smf/internal/core"
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
