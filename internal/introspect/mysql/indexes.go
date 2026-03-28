package mysql

import (
	"errors"
	"strconv"
	"strings"

	"smf/internal/introspect"
	"smf/internal/schema"
)

// parseIndex parses an inline index declaration from a CREATE TABLE body item.
//
// Handles: KEY, INDEX, FULLTEXT KEY/INDEX, SPATIAL KEY/INDEX, UNIQUE KEY/INDEX.
func parseIndex(_ schema.Dialect, item string) (*schema.Index, error) {
	tokens := introspect.Tokenize(item)
	if len(tokens) == 0 {
		return nil, errors.New("empty index declaration")
	}

	idx := &schema.Index{}
	var i int
	i = parseIndexPrefix(tokens, i, idx)

	// Index name
	if i < len(tokens) && !strings.EqualFold(tokens[i], "USING") && !strings.HasPrefix(tokens[i], "(") {
		idx.Name = introspect.StripQuotes(tokens[i])
		i++
	}

	// USING index_type before columns
	if i < len(tokens) && strings.EqualFold(tokens[i], "USING") {
		i++
		if i < len(tokens) {
			idx.Type = schema.IndexType(strings.ToUpper(tokens[i]))
			i++
		}
	}

	// Columns
	if i < len(tokens) && strings.HasPrefix(tokens[i], "(") {
		parseIndexColumns(idx, tokens[i])
		i++
	}

	// Options
	parseIndexOptions(tokens, i, idx)

	return idx, nil
}

func parseIndexPrefix(tokens []string, i int, idx *schema.Index) int {
	if i >= len(tokens) {
		return i
	}

	switch strings.ToUpper(tokens[i]) {
	case "UNIQUE":
		idx.Unique = true
		i++
	case "FULLTEXT":
		idx.Type = schema.IndexTypeFullText
		i++
	case "SPATIAL":
		idx.Type = schema.IndexTypeSpatial
		i++
	}

	if i < len(tokens) && (strings.EqualFold(tokens[i], "INDEX") || strings.EqualFold(tokens[i], "KEY")) {
		i++
	}
	return i
}

func parseIndexOptions(tokens []string, i int, idx *schema.Index) {
	for i < len(tokens) {
		token := strings.ToUpper(tokens[i])
		switch token {
		case "USING":
			i++
			if i < len(tokens) {
				idx.Type = schema.IndexType(strings.ToUpper(tokens[i]))
			}
		case "COMMENT":
			i++
			if i < len(tokens) {
				idx.Comment = introspect.StripQuotes(tokens[i])
			}
		case "VISIBLE":
			idx.Visibility = schema.IndexVisible
		case "INVISIBLE":
			idx.Visibility = schema.IndexInvisible
		}
		i++
	}
}

func parseIndexColumns(idx *schema.Index, colsStr string) {
	colsStr = strings.TrimPrefix(colsStr, "(")
	colsStr = strings.TrimSuffix(colsStr, ")")

	colTokens := introspect.SplitBy(colsStr, ',')
	for _, colToken := range colTokens {
		colToken = strings.TrimSpace(colToken)
		parts := introspect.Tokenize(colToken)
		if len(parts) > 0 {
			colIdx := schema.ColumnIndex{}
			namePart := parts[0]

			if parenIdx := strings.Index(namePart, "("); parenIdx != -1 {
				colIdx.Name = introspect.StripQuotes(namePart[:parenIdx])
				lenStr := strings.Trim(namePart[parenIdx:], "()")
				if l, err := strconv.Atoi(lenStr); err == nil {
					colIdx.Length = l
				}
			} else {
				colIdx.Name = introspect.StripQuotes(namePart)
			}

			for j := 1; j < len(parts); j++ {
				part := strings.ToUpper(parts[j])
				switch {
				case strings.HasPrefix(part, "("):
					lenStr := strings.Trim(part, "()")
					if l, err := strconv.Atoi(lenStr); err == nil {
						colIdx.Length = l
					}
				case part == "ASC":
					colIdx.Order = schema.SortAsc
				case part == "DESC":
					colIdx.Order = schema.SortDesc
				}
			}

			idx.Columns = append(idx.Columns, colIdx)
		}
	}
}

