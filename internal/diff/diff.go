// Package diff provides functionality to compare and generate schema diffs between two schema dumps.
// It also includes breaking changes detection.
package diff

import (
	"smf/internal/core"
)

const (
	// renameDetectionScoreThreshold is the minimum similarity score required to consider
	// a removed+added column pair as a rename. The score is computed by comparing column
	// attributes (type=4pts, nullable=1pt, auto_increment=1pt, etc.). A threshold of 12
	// requires near-identical column definitions to avoid false positives.
	renameDetectionScoreThreshold = 12

	// renameSharedTokenMinLen is the minimum length of shared name tokens (e.g., "user" in
	// "user_id" and "user_name") required as additional evidence for rename detection.
	renameSharedTokenMinLen = 3
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

// ColumnRename represents the score of column rename detection.
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

// GetName methods implement the Named interface for type-safe sorting.
func (td *TableDiff) GetName() string          { return td.Name }
func (cc *ColumnChange) GetName() string       { return cc.Name }
func (cc *ConstraintChange) GetName() string   { return cc.Name }
func (ic *IndexChange) GetName() string        { return ic.Name }
func (toc *TableOptionChange) GetName() string { return toc.Name }

type Options struct {
	DetectColumnRenames bool
}

// DefaultOptions are options for mysql diffing
func DefaultOptions() Options {
	return Options{DetectColumnRenames: true}
}

// Diff compares two database dumps and returns a SchemaDiff object.
// NOTE: For very large schemas (100+ tables), table comparisons could be parallelized,
// but current sequential approach is sufficient for typical use cases.
func Diff(oldDB, newDB *core.Database, opts Options) *SchemaDiff {
	d := &SchemaDiff{}
	oldTables, oldCollisions := mapTablesByName(oldDB.Tables)
	newTables, newCollisions := mapTablesByName(newDB.Tables)
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

	sortNamed(d.AddedTables)
	sortNamed(d.RemovedTables)
	sortNamed(d.ModifiedTables)

	return d
}

// IsEmpty returns true if there are no differences in the schema diff.
func (d *SchemaDiff) IsEmpty() bool {
	return len(d.AddedTables) == 0 && len(d.RemovedTables) == 0 && len(d.ModifiedTables) == 0
}
