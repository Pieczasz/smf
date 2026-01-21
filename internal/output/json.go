package output

import (
	"encoding/json"

	"smf/internal/core"
	"smf/internal/diff"
	"smf/internal/migration"
)

type jsonFormatter struct{}

type diffSummary struct {
	AddedTables    int `json:"addedTables"`
	RemovedTables  int `json:"removedTables"`
	ModifiedTables int `json:"modifiedTables"`
}

type diffPayload struct {
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
	payload := diffPayload{Format: string(FormatJSON)}
	if d != nil {
		payload.Warnings = d.Warnings
		payload.AddedTables = d.AddedTables
		payload.RemovedTables = d.RemovedTables
		payload.ModifiedTables = d.ModifiedTables
		payload.Summary = diffSummary{
			AddedTables:    len(d.AddedTables),
			RemovedTables:  len(d.RemovedTables),
			ModifiedTables: len(d.ModifiedTables),
		}
	}
	return marshalJSON(payload)
}

// FormatMigration formats a migration in JSON format.
func (jsonFormatter) FormatMigration(m *migration.Migration) (string, error) {
	payload := migrationPayload{Format: string(FormatJSON)}
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
