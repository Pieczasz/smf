// Package output provides a set of formatters for schema diffs and migrations.
// It is extendable and for now provides two formats: human and JSON.
package output

import (
	"fmt"
	"strings"

	"smf/internal/diff"
	"smf/internal/migration"
)

// Format is an enum type representing the available output formats.
type Format string

const (
	FormatHuman Format = "human"
	FormatJSON  Format = "json"
)

// Formatter is an interface for formatting schema diffs and migrations.
type Formatter interface {
	FormatDiff(*diff.SchemaDiff) (string, error)
	FormatMigration(*migration.Migration) (string, error)
}

// NewFormatter creates a new Formatter instance based on the given name.
// If no format is specified, defaults to human-readable format.
func NewFormatter(name string) (Formatter, error) {
	format := Format(strings.ToLower(strings.TrimSpace(name)))
	switch format {
	case "", FormatHuman:
		return humanFormatter{}, nil
	case FormatJSON:
		return jsonFormatter{}, nil
	default:
		return nil, fmt.Errorf("unsupported format: %s; use 'human' or 'json'", name)
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
