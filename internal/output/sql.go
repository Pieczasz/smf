package output

import (
	"io"
	"strings"

	"smf/internal/core"
	"smf/internal/diff"
	"smf/internal/migration"
)

type sqlFormatter struct{}

// FormatDiff formats a schema diff in SQL format.
func (sqlFormatter) FormatDiff(d *diff.SchemaDiff) (string, error) {
	if d == nil {
		return "", nil
	}
	return formatDiffText(d), nil
}

// FormatMigration formats a migration in SQL format.
func (sqlFormatter) FormatMigration(m *migration.Migration) (string, error) {
	if m == nil {
		return "", nil
	}

	var sb strings.Builder
	sb.WriteString("-- smf migration\n")
	sb.WriteString("-- Review before running in production.\n")

	writeCommentSection(&sb, "BREAKING CHANGES (manual review required)", m.BreakingNotes())
	writeCommentSection(&sb, "UNRESOLVED (cannot auto-generate safely)", m.UnresolvedNotes())
	writeCommentSection(&sb, "NOTES", m.InfoNotes())

	sqlOps := getSQLOperations(m)
	rb := m.RollbackStatements()

	if len(sqlOps) == 0 {
		return formatEmptyMigration(&sb, rb), nil
	}

	writeSQLOperations(&sb, sqlOps)

	if len(rb) > 0 {
		sb.WriteString("\n-- ROLLBACK SQL (run separately)\n")
		writeRollbackAsComments(&sb, rb)
	}

	return sb.String(), nil
}

func formatEmptyMigration(sb *strings.Builder, rb []string) string {
	sb.WriteString("\n-- No SQL statements generated.\n")
	if len(rb) > 0 {
		sb.WriteString("\n-- ROLLBACK SQL (run separately if needed)\n")
		writeRollbackAsComments(sb, rb)
	}
	return sb.String()
}

func writeSQLOperations(sb *strings.Builder, sqlOps []core.Operation) {
	sb.WriteString("\n-- SQL\n")
	for _, op := range sqlOps {
		if op.SQL == "" {
			continue
		}
		writeRiskComment(sb, op)
		sb.WriteString(op.SQL)
		if !strings.HasSuffix(op.SQL, ";") {
			sb.WriteString(";")
		}
		sb.WriteString("\n")
	}
}

func writeRiskComment(sb *strings.Builder, op core.Operation) {
	if op.Risk != "" && op.Risk != core.RiskInfo {
		sb.WriteString("-- [" + string(op.Risk) + "]")
		if op.RequiresLock {
			sb.WriteString(" (may acquire locks)")
		}
		sb.WriteString("\n")
	}
}

// FormatRollbackSQL formats a migration's rollback statements as SQL.
func FormatRollbackSQL(m *migration.Migration) string {
	if m == nil {
		return ""
	}

	var sb strings.Builder
	sb.WriteString("-- smf rollback\n")
	sb.WriteString("-- Run to revert the migration (review carefully).\n")

	rb := m.RollbackStatements()
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

// WriteMigration writes a formatted migration to the given writer.
// TODO: use this writer
func WriteMigration(m *migration.Migration, w io.Writer) error {
	content, err := sqlFormatter{}.FormatMigration(m)
	if err != nil {
		return err
	}
	_, err = io.WriteString(w, content)
	return err
}

// WriteRollback writes formatted rollback SQL to the given writer.
func WriteRollback(m *migration.Migration, w io.Writer) error {
	_, err := io.WriteString(w, FormatRollbackSQL(m))
	return err
}

func getSQLOperations(m *migration.Migration) []core.Operation {
	var ops []core.Operation
	for _, op := range m.Plan() {
		if op.Kind == core.OperationSQL && op.SQL != "" {
			ops = append(ops, op)
		}
	}
	return ops
}

func writeCommentSection(sb *strings.Builder, title string, items []string) {
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

func splitCommentLines(s string) []string {
	s = strings.ReplaceAll(s, "\r\n", "\n")
	s = strings.ReplaceAll(s, "\r", "\n")
	lines := strings.Split(s, "\n")
	for i, line := range lines {
		lines[i] = strings.TrimSpace(line)
	}
	return lines
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
