// Package diff provides functionality to compare and generate schema diffs between two schema dumps.
// It also includes breaking changes detection.
package diff

import (
	"smf/internal/core"
)

const (
	renameDetectionScoreThreshold = 12
	renameSharedTokenMinLen       = 3
)

// SchemaDiff represents the differences between two schema dumps.
type SchemaDiff struct {
	Warnings       []string `json:"warnings,omitempty"`
	AddedTables    []*core.Table
	RemovedTables  []*core.Table
	ModifiedTables []*TableDiff
}

// TableDiff represents the differences between two tables.
type TableDiff struct {
	Name                string
	Warnings            []string `json:"warnings,omitempty"`
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

// ColumnChange represents the differences between two columns.
type ColumnChange struct {
	Name    string
	Old     *core.Column
	New     *core.Column
	Changes []*FieldChange
}

// ColumnRename represents the score of a column rename detection.
type ColumnRename struct {
	Old   *core.Column
	New   *core.Column
	Score int
}

// ConstraintChange represents the constraint difference between old table and new table.
type ConstraintChange struct {
	Name          string
	Old           *core.Constraint
	New           *core.Constraint
	Changes       []*FieldChange
	RebuildOnly   bool
	RebuildReason string
}

// IndexChange represents the differences between indexes of old table and new table.
type IndexChange struct {
	Name    string
	Old     *core.Index
	New     *core.Index
	Changes []*FieldChange
}

// FieldChange represents the differences between two fields.
type FieldChange struct {
	Field string
	Old   string
	New   string
}

// TableOptionChange represents the differences between two table options.
type TableOptionChange struct {
	Name string
	Old  string
	New  string
}

type Options struct {
	DetectColumnRenames bool
}

func DefaultOptions() Options {
	return Options{DetectColumnRenames: true}
}

// Diff compares two database dumps and returns a SchemaDiff object.
func Diff(oldDB, newDB *core.Database) *SchemaDiff {
	return DiffWithOptions(oldDB, newDB, DefaultOptions())
}

// DiffWithOptions compares two database dumps and returns a SchemaDiff object.
func DiffWithOptions(oldDB, newDB *core.Database, opts Options) *SchemaDiff {
	d := &SchemaDiff{}

	oldTables, oldCollisions := mapByLowerNameWithCollisions(oldDB.Tables, func(t *core.Table) string { return t.Name })
	newTables, newCollisions := mapByLowerNameWithCollisions(newDB.Tables, func(t *core.Table) string { return t.Name })
	for _, c := range oldCollisions {
		d.Warnings = append(d.Warnings, "old schema: "+c)
	}
	for _, c := range newCollisions {
		d.Warnings = append(d.Warnings, "new schema: "+c)
	}

	for name, nt := range newTables {
		ot, ok := oldTables[name]
		if !ok {
			d.AddedTables = append(d.AddedTables, nt)
			continue
		}

		td := compareTable(ot, nt, opts)
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
