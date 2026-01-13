package mysql

import (
	"fmt"
	"smf/core"
	"strconv"
	"strings"
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
	for _, kw := range keywords {
		if upper == kw {
			return upper
		}
	}

	if looksNumeric(v) {
		return v
	}

	if strings.ContainsAny(v, "()") {
		return v
	}

	return g.QuoteString(v)
}

func looksNumeric(s string) bool {
	_, err := strconv.ParseFloat(s, 64)
	return err == nil
}
