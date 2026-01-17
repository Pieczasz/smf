package output

import (
	"smf/internal/diff"
	"smf/internal/migration"
)

type humanFormatter struct{}

func (humanFormatter) FormatDiff(d *diff.SchemaDiff) (string, error) {
	if d == nil {
		return "", nil
	}
	return d.String(), nil
}

func (humanFormatter) FormatMigration(m *migration.Migration) (string, error) {
	if m == nil {
		return "", nil
	}
	return m.String(), nil
}
