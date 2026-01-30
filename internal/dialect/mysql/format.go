package mysql

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	"smf/internal/core"
)

func (g *Generator) formatColumns(cols []string) string {
	var quoted []string
	for _, c := range cols {
		c = strings.TrimSpace(c)
		if c == "" {
			continue
		}
		quoted = append(quoted, g.QuoteIdentifier(c))
	}
	return "(" + strings.Join(quoted, ", ") + ")"
}

func (g *Generator) formatIndexColumns(cols []core.IndexColumn) string {
	var quoted []string
	for _, c := range cols {
		name := strings.TrimSpace(c.Name)
		if name == "" {
			continue
		}
		qname := g.QuoteIdentifier(name)
		if c.Length > 0 {
			qname = fmt.Sprintf("%s(%d)", qname, c.Length)
		}
		quoted = append(quoted, qname)
	}
	return "(" + strings.Join(quoted, ", ") + ")"
}

func (g *Generator) formatValue(v string) string {
	v = strings.TrimSpace(v)
	if v == "" {
		return "''"
	}

	upper := strings.ToUpper(v)
	keywords := []string{"NULL", "CURRENT_TIMESTAMP", "CURRENT_DATE", "CURRENT_TIME", "NOW()", "TRUE", "FALSE"}
	if slices.Contains(keywords, upper) {
		return upper
	}

	if _, err := strconv.ParseFloat(v, 64); err == nil {
		return v
	}

	if strings.ContainsAny(v, "()") {
		return v
	}

	return g.QuoteString(v)
}
