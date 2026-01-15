package migration

import (
	"os"
	"smf/core"
	"strings"
)

type Migration struct {
	Operations []core.Operation
}

func (m *Migration) Plan() []core.Operation {
	if m == nil {
		return nil
	}
	return m.Operations
}

func (m *Migration) SQLStatements() []string {
	return m.sqlStatements()
}

func (m *Migration) RollbackStatements() []string {
	return m.rollbackStatements()
}

func (m *Migration) BreakingNotes() []string {
	return m.breakingNotes()
}

func (m *Migration) UnresolvedNotes() []string {
	return m.unresolvedNotes()
}

func (m *Migration) InfoNotes() []string {
	return m.infoNotes()
}

func (m *Migration) sqlStatements() []string {
	if m == nil {
		return nil
	}
	var out []string
	for _, op := range m.Operations {
		if op.Kind != core.OperationSQL {
			continue
		}
		stmt := strings.TrimSpace(op.SQL)
		if stmt == "" {
			continue
		}
		out = append(out, stmt)
	}
	return out
}

func (m *Migration) rollbackStatements() []string {
	if m == nil {
		return nil
	}
	var out []string
	for _, op := range m.Operations {
		if op.Kind != core.OperationSQL {
			continue
		}
		stmt := strings.TrimSpace(op.RollbackSQL)
		if stmt == "" {
			continue
		}
		out = append(out, stmt)
	}
	return out
}

func (m *Migration) breakingNotes() []string {
	if m == nil {
		return nil
	}
	var out []string
	for _, op := range m.Operations {
		if op.Kind != core.OperationBreaking {
			continue
		}
		msg := strings.TrimSpace(op.SQL)
		if msg == "" {
			continue
		}
		out = append(out, msg)
	}
	return out
}

func (m *Migration) unresolvedNotes() []string {
	if m == nil {
		return nil
	}
	var out []string
	for _, op := range m.Operations {
		if op.Kind != core.OperationUnresolved {
			continue
		}
		msg := strings.TrimSpace(op.UnresolvedReason)
		if msg == "" {
			continue
		}
		out = append(out, msg)
	}
	return out
}

func (m *Migration) infoNotes() []string {
	if m == nil {
		return nil
	}
	var out []string
	for _, op := range m.Operations {
		if op.Kind != core.OperationNote {
			continue
		}
		msg := strings.TrimSpace(op.SQL)
		if msg == "" {
			continue
		}
		out = append(out, msg)
	}
	return out
}

func (m *Migration) String() string {
	var sb strings.Builder
	sb.WriteString("-- smf migration\n")
	sb.WriteString("-- Review before running in production.\n")

	writeCommentSection := func(title string, items []string) {
		if len(items) == 0 {
			return
		}
		sb.WriteString("\n-- " + title + "\n")
		for _, item := range items {
			for _, line := range splitCommentLines(item) {
				if line == "" {
					continue
				}
				sb.WriteString("-- - " + line + "\n")
			}
		}
	}

	writeCommentSection("BREAKING CHANGES (manual review required)", m.breakingNotes())
	writeCommentSection("UNRESOLVED (cannot auto-generate safely)", m.unresolvedNotes())
	writeCommentSection("NOTES", m.infoNotes())

	sql := m.sqlStatements()
	rb := m.rollbackStatements()

	if len(sql) == 0 {
		sb.WriteString("\n-- No SQL statements generated.\n")
		if len(rb) > 0 {
			sb.WriteString("\n-- ROLLBACK SQL (run separately)\n")
			writeRollbackAsComments(&sb, rb)
		}
		return sb.String()
	}

	sb.WriteString("\n-- SQL\n")
	for _, stmt := range sql {
		if stmt == "" {
			continue
		}
		sb.WriteString(stmt)
		if !strings.HasSuffix(stmt, ";") {
			sb.WriteString(";")
		}
		sb.WriteString("\n")
	}

	if len(rb) > 0 {
		sb.WriteString("\n-- ROLLBACK SQL (run separately)\n")
		writeRollbackAsComments(&sb, rb)
	}

	return sb.String()
}

func (m *Migration) RollbackString() string {
	var sb strings.Builder
	sb.WriteString("-- smf rollback\n")
	sb.WriteString("-- Run to revert the migration (review carefully).\n")

	rb := m.rollbackStatements()
	if len(rb) == 0 {
		sb.WriteString("\n-- No rollback statements generated.\n")
		return sb.String()
	}

	sb.WriteString("\n-- SQL\n")
	for i := len(rb) - 1; i >= 0; i-- {
		stmt := strings.TrimSpace(rb[i])
		if stmt == "" {
			continue
		}
		sb.WriteString(stmt)
		if !strings.HasSuffix(stmt, ";") {
			sb.WriteString(";")
		}
		sb.WriteString("\n")
	}

	return sb.String()
}

func splitCommentLines(s string) []string {
	s = strings.ReplaceAll(s, "\r\n", "\n")
	s = strings.ReplaceAll(s, "\r", "\n")
	lines := strings.Split(s, "\n")
	for i, line := range lines {
		lines[i] = strings.TrimSpace(line)
	}
	return lines
}

func (m *Migration) SaveToFile(path string) error {
	return os.WriteFile(path, []byte(m.String()), 0644)
}

func (m *Migration) SaveRollbackToFile(path string) error {
	return os.WriteFile(path, []byte(m.RollbackString()), 0644)
}

func (m *Migration) AddStatement(stmt string) {
	if m == nil {
		return
	}
	if stmt = strings.TrimSpace(stmt); stmt == "" {
		return
	}
	m.Operations = append(m.Operations, core.Operation{Kind: core.OperationSQL, SQL: stmt})
}

func (m *Migration) AddRollbackStatement(stmt string) {
	if m == nil {
		return
	}
	if stmt = strings.TrimSpace(stmt); stmt == "" {
		return
	}
	m.Operations = append(m.Operations, core.Operation{Kind: core.OperationSQL, RollbackSQL: stmt})
}

func (m *Migration) AddStatementWithRollback(up, down string) {
	if m == nil {
		return
	}
	up = strings.TrimSpace(up)
	down = strings.TrimSpace(down)
	if up == "" && down == "" {
		return
	}
	m.Operations = append(m.Operations, core.Operation{Kind: core.OperationSQL, SQL: up, RollbackSQL: down})
}

func (m *Migration) AddBreaking(msg string) {
	if m == nil {
		return
	}
	if msg = strings.TrimSpace(msg); msg == "" {
		return
	}
	m.Operations = append(m.Operations, core.Operation{Kind: core.OperationBreaking, SQL: msg, Risk: core.RiskBreaking})
}

func (m *Migration) AddNote(msg string) {
	if m == nil {
		return
	}
	if msg = strings.TrimSpace(msg); msg == "" {
		return
	}
	m.Operations = append(m.Operations, core.Operation{Kind: core.OperationNote, SQL: msg, Risk: core.RiskInfo})
}

func (m *Migration) AddUnresolved(msg string) {
	if m == nil {
		return
	}
	if msg = strings.TrimSpace(msg); msg == "" {
		return
	}
	m.Operations = append(m.Operations, core.Operation{Kind: core.OperationUnresolved, UnresolvedReason: msg})
}

func (m *Migration) Dedupe() {
	if m == nil {
		return
	}
	seenNote := make(map[string]struct{})
	seenBreaking := make(map[string]struct{})
	seenUnresolved := make(map[string]struct{})
	seenRollback := make(map[string]struct{})
	var out []core.Operation
	for _, op := range m.Operations {
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

func writeRollbackAsComments(sb *strings.Builder, rollback []string) {
	for i := len(rollback) - 1; i >= 0; i-- {
		for _, line := range splitCommentLines(rollback[i]) {
			if line == "" {
				continue
			}
			sb.WriteString("-- ")
			sb.WriteString(line)
			if !strings.HasSuffix(line, ";") {
				sb.WriteString(";")
			}
			sb.WriteString("\n")
		}
	}
}
