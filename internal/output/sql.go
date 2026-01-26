package output

import (
	"os"
	"strings"

	"smf/internal/diff"
	"smf/internal/migration"
)

type sqlFormatter struct{}

// FormatDiff formats a schema diff in SQL format.
// TODO: move diff formatting to this package, instead of using d.String()
func (sqlFormatter) FormatDiff(d *diff.SchemaDiff) (string, error) {
	if d == nil {
		return "", nil
	}
	return d.String(), nil
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

	sql := m.SQLStatements()
	rb := m.RollbackStatements()

	if len(sql) == 0 {
		sb.WriteString("\n-- No SQL statements generated.\n")
		if len(rb) > 0 {
			sb.WriteString("\n-- ROLLBACK SQL (run separately if needed)\n")
			writeRollbackAsComments(&sb, rb)
		}
		return sb.String(), nil
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

	return sb.String(), nil
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

// SaveMigrationToFile saves a formatted migration to a file.
func SaveMigrationToFile(m *migration.Migration, path string) error {
	content, err := sqlFormatter{}.FormatMigration(m)
	if err != nil {
		return err
	}
	return os.WriteFile(path, []byte(content), 0644)
}

// SaveRollbackToFile saves a formatted rollback migration to a file.
func SaveRollbackToFile(m *migration.Migration, path string) error {
	return os.WriteFile(path, []byte(FormatRollbackSQL(m)), 0644)
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
