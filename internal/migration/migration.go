// Package migration provides a way to define and execute database migrations.
// It is designed to be used with the smf/internal/core package.
package migration

import (
	"strings"

	"smf/internal/core"
)

// Migration struct contains all operations that need to be performed
// to apply a schema migration.
type Migration struct {
	Operations []core.Operation
}

// Plan returns the list of operations that needs to be performed,
// to apply a schema migration.
func (m *Migration) Plan() []core.Operation {
	return m.Operations
}

// SQLStatements returns the list of SQL statements that needs to be executed,
// to apply a schema migration.
func (m *Migration) SQLStatements() []string {
	return m.sqlStatements()
}

// RollbackStatements returns the list of SQL statements that needs to be executed,
// to rollback a schema migration.
func (m *Migration) RollbackStatements() []string {
	return m.rollbackStatements()
}

// BreakingNotes returns the list of notes that needs to be addressed,
// to apply a schema migration. The notes refer to breaking changes in the schema.
func (m *Migration) BreakingNotes() []string {
	return m.breakingNotes()
}

// UnresolvedNotes returns the list of notes that needs to be addressed,
// to apply a schema migration. The notes refer to unresolved issues in the schema.
func (m *Migration) UnresolvedNotes() []string {
	return m.unresolvedNotes()
}

// InfoNotes returns the list of notes that needs to be addressed,
// to apply a schema migration. The notes are information for the user.
func (m *Migration) InfoNotes() []string {
	return m.infoNotes()
}

func (m *Migration) AddStatement(stmt string) {
	if stmt = strings.TrimSpace(stmt); stmt == "" {
		return
	}
	m.Operations = append(m.Operations, core.Operation{Kind: core.OperationSQL, SQL: stmt})
}

func (m *Migration) AddRollbackStatement(stmt string) {
	if stmt = strings.TrimSpace(stmt); stmt == "" {
		return
	}
	m.Operations = append(m.Operations, core.Operation{Kind: core.OperationSQL, RollbackSQL: stmt})
}

func (m *Migration) AddStatementWithRollback(up, down string) {
	up = strings.TrimSpace(up)
	down = strings.TrimSpace(down)
	if up == "" && down == "" {
		return
	}
	m.Operations = append(m.Operations, core.Operation{Kind: core.OperationSQL, SQL: up, RollbackSQL: down})
}

func (m *Migration) AddBreaking(msg string) {
	if msg = strings.TrimSpace(msg); msg == "" {
		return
	}
	m.Operations = append(m.Operations, core.Operation{Kind: core.OperationBreaking, SQL: msg, Risk: core.RiskBreaking})
}

func (m *Migration) AddNote(msg string) {
	if msg = strings.TrimSpace(msg); msg == "" {
		return
	}
	m.Operations = append(m.Operations, core.Operation{Kind: core.OperationNote, SQL: msg, Risk: core.RiskInfo})
}

func (m *Migration) AddUnresolved(msg string) {
	if msg = strings.TrimSpace(msg); msg == "" {
		return
	}
	m.Operations = append(m.Operations, core.Operation{Kind: core.OperationUnresolved, UnresolvedReason: msg})
}

func (m *Migration) Dedupe() {
	n := len(m.Operations)
	if n == 0 {
		return
	}
	seenNote := make(map[string]struct{}, n)
	seenBreaking := make(map[string]struct{}, n)
	seenUnresolved := make(map[string]struct{}, n)
	seenRollback := make(map[string]struct{}, n)
	out := make([]core.Operation, 0, n)
	for i := range m.Operations {
		op := m.Operations[i]
		op.SQL = strings.TrimSpace(op.SQL)
		op.RollbackSQL = strings.TrimSpace(op.RollbackSQL)
		op.UnresolvedReason = strings.TrimSpace(op.UnresolvedReason)

		switch op.Kind {
		case core.OperationSQL:
			if op.SQL == "" && op.RollbackSQL == "" {
				continue
			}
			if op.RollbackSQL != "" {
				if _, ok := seenRollback[op.RollbackSQL]; ok {
					op.RollbackSQL = ""
				} else {
					seenRollback[op.RollbackSQL] = struct{}{}
				}
			}
			out = append(out, op)
		case core.OperationNote:
			if op.SQL == "" {
				continue
			}
			if _, ok := seenNote[op.SQL]; ok {
				continue
			}
			seenNote[op.SQL] = struct{}{}
			out = append(out, op)
		case core.OperationBreaking:
			if op.SQL == "" {
				continue
			}
			if _, ok := seenBreaking[op.SQL]; ok {
				continue
			}
			seenBreaking[op.SQL] = struct{}{}
			out = append(out, op)
		case core.OperationUnresolved:
			if op.UnresolvedReason == "" {
				continue
			}
			if _, ok := seenUnresolved[op.UnresolvedReason]; ok {
				continue
			}
			seenUnresolved[op.UnresolvedReason] = struct{}{}
			out = append(out, op)
		default:
			out = append(out, op)
		}
	}
	m.Operations = out
}

func (m *Migration) filterByKind(kind core.OperationKind, fieldFn func(core.Operation) string) []string {
	out := make([]string, 0, len(m.Operations)/4+1)
	for i := range m.Operations {
		op := &m.Operations[i]
		if op.Kind != kind {
			continue
		}
		val := strings.TrimSpace(fieldFn(*op))
		if val == "" {
			continue
		}
		out = append(out, val)
	}
	return out
}

func (m *Migration) sqlStatements() []string {
	return m.filterByKind(core.OperationSQL, func(op core.Operation) string { return op.SQL })
}

func (m *Migration) rollbackStatements() []string {
	return m.filterByKind(core.OperationSQL, func(op core.Operation) string { return op.RollbackSQL })
}

func (m *Migration) breakingNotes() []string {
	return m.filterByKind(core.OperationBreaking, func(op core.Operation) string { return op.SQL })
}

func (m *Migration) unresolvedNotes() []string {
	return m.filterByKind(core.OperationUnresolved, func(op core.Operation) string { return op.UnresolvedReason })
}

func (m *Migration) infoNotes() []string {
	return m.filterByKind(core.OperationNote, func(op core.Operation) string { return op.SQL })
}
