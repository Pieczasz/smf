package output

import (
	"encoding/json"

	"smf/internal/core"
	"smf/internal/diff"
	"smf/internal/migration"
)

// FormatVersion is the current version of the JSON output format.
// Increment this when making breaking changes to the JSON structure.
const FormatVersion = "1.0"

type jsonFormatter struct{}

type diffSummary struct {
	AddedTables     int `json:"addedTables"`
	RemovedTables   int `json:"removedTables"`
	ModifiedTables  int `json:"modifiedTables"`
	AddedColumns    int `json:"addedColumns"`
	RemovedColumns  int `json:"removedColumns"`
	ModifiedColumns int `json:"modifiedColumns"`
	AddedIndexes    int `json:"addedIndexes"`
	RemovedIndexes  int `json:"removedIndexes"`
}

type diffPayload struct {
	FormatVersion  string            `json:"formatVersion"`
	Format         string            `json:"format"`
	Summary        diffSummary       `json:"summary"`
	Warnings       []string          `json:"warnings,omitempty"`
	AddedTables    []*core.Table     `json:"addedTables,omitempty"`
	RemovedTables  []*core.Table     `json:"removedTables,omitempty"`
	ModifiedTables []*diff.TableDiff `json:"modifiedTables,omitempty"`
}

type migrationSummary struct {
	BreakingChanges    int `json:"breakingChanges"`
	Unresolved         int `json:"unresolved"`
	Notes              int `json:"notes"`
	SQLStatements      int `json:"sqlStatements"`
	RollbackStatements int `json:"rollbackStatements"`
}

type migrationPayload struct {
	FormatVersion   string           `json:"formatVersion"`
	Format          string           `json:"format"`
	Summary         migrationSummary `json:"summary"`
	BreakingChanges []string         `json:"breakingChanges,omitempty"`
	Unresolved      []string         `json:"unresolved,omitempty"`
	Notes           []string         `json:"notes,omitempty"`
	SQL             []string         `json:"sql,omitempty"`
	Rollback        []string         `json:"rollback,omitempty"`
}

// Payload interface is used as a generic interface to marshal JSON payload only for this interface.
type Payload interface {
	diffPayload | migrationPayload
}

// FormatDiff formats a schema diff in JSON format.
func (jsonFormatter) FormatDiff(d *diff.SchemaDiff) (string, error) {
	payload := diffPayload{
		FormatVersion: FormatVersion,
		Format:        string(FormatJSON),
	}
	if d != nil {
		normalizeTableOptions(d.AddedTables)
		normalizeTableOptions(d.RemovedTables)
		payload.Warnings = d.Warnings
		payload.AddedTables = d.AddedTables
		payload.RemovedTables = d.RemovedTables
		payload.ModifiedTables = d.ModifiedTables
		payload.Summary = computeDiffSummary(d)
	}
	return marshalJSON(payload)
}

func normalizeTableOptions(tables []*core.Table) {
	for _, t := range tables {
		if t == nil {
			continue
		}
		if t.Options.MySQL == nil {
			t.Options.MySQL = &core.MySQLTableOptions{}
		}
		if t.Options.TiDB == nil {
			t.Options.TiDB = &core.TiDBTableOptions{}
		}
	}
}

func computeDiffSummary(d *diff.SchemaDiff) diffSummary {
	summary := diffSummary{
		AddedTables:    len(d.AddedTables),
		RemovedTables:  len(d.RemovedTables),
		ModifiedTables: len(d.ModifiedTables),
	}

	for _, td := range d.ModifiedTables {
		summary.AddedColumns += len(td.AddedColumns)
		summary.RemovedColumns += len(td.RemovedColumns)
		summary.ModifiedColumns += len(td.ModifiedColumns)
		summary.AddedIndexes += len(td.AddedIndexes)
		summary.RemovedIndexes += len(td.RemovedIndexes)
	}

	for _, t := range d.AddedTables {
		summary.AddedColumns += len(t.Columns)
		summary.AddedIndexes += len(t.Indexes)
	}

	for _, t := range d.RemovedTables {
		summary.RemovedColumns += len(t.Columns)
		summary.RemovedIndexes += len(t.Indexes)
	}

	return summary
}

// FormatMigration formats a migration in JSON format.
func (jsonFormatter) FormatMigration(m *migration.Migration) (string, error) {
	payload := migrationPayload{
		FormatVersion: FormatVersion,
		Format:        string(FormatJSON),
	}
	if m != nil {
		breaking := m.BreakingNotes()
		unresolved := m.UnresolvedNotes()
		notes := m.InfoNotes()
		sql := normalizeStatements(m.SQLStatements())
		rollback := normalizeStatements(reverseStatements(m.RollbackStatements()))

		payload.BreakingChanges = breaking
		payload.Unresolved = unresolved
		payload.Notes = notes
		payload.SQL = sql
		payload.Rollback = rollback
		payload.Summary = migrationSummary{
			BreakingChanges:    len(breaking),
			Unresolved:         len(unresolved),
			Notes:              len(notes),
			SQLStatements:      len(sql),
			RollbackStatements: len(rollback),
		}
	}
	return marshalJSON(payload)
}

func marshalJSON[T Payload](payload T) (string, error) {
	b, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return "", err
	}
	return string(b) + "\n", nil
}
