package output

import (
	"fmt"
	"smf/diff"
	"smf/migration"
	"strings"
)

type Format string

const (
	FormatHuman Format = "human"
	FormatJSON  Format = "json"
)

type Formatter interface {
	FormatDiff(*diff.SchemaDiff) (string, error)
	FormatMigration(*migration.Migration) (string, error)
}

func NewFormatter(name string) (Formatter, error) {
	format := Format(strings.ToLower(strings.TrimSpace(name)))
	switch format {
	case "", FormatJSON:
		return jsonFormatter{}, nil
	case FormatHuman:
		return humanFormatter{}, nil
	default:
		return nil, fmt.Errorf("unsupported format: %s", name)
	}
}

func normalizeStatements(stmts []string) []string {
	var out []string
	for _, stmt := range stmts {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		if !strings.HasSuffix(stmt, ";") {
			stmt += ";"
		}
		out = append(out, stmt)
	}
	return out
}

func reverseStatements(stmts []string) []string {
	if len(stmts) == 0 {
		return nil
	}
	out := make([]string, 0, len(stmts))
	for i := len(stmts) - 1; i >= 0; i-- {
		out = append(out, stmts[i])
	}
	return out
}
