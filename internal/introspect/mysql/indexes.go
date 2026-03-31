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
		switch {
		case token == "USING":
			i = parseUsingOption(tokens, i, idx)
		case token == "COMMENT":
			i = parseCommentOption(tokens, i, idx)
		case token == "VISIBLE":
			idx.Visibility = schema.IndexVisible
		case token == "INVISIBLE":
			idx.Visibility = schema.IndexInvisible
		case token == "KEY_BLOCK_SIZE" || strings.HasPrefix(token, "KEY_BLOCK_SIZE="):
			i = parseKeyBlockSizeOption(tokens, i, token, idx)
		case token == "WITH":
			i = parseWithParserOption(tokens, i, idx)
		}
		i++
	}
}

func parseUsingOption(tokens []string, i int, idx *schema.Index) int {
	if i+1 < len(tokens) {
		idx.Type = schema.IndexType(strings.ToUpper(tokens[i+1]))
		return i + 1
	}
	return i
}

func parseCommentOption(tokens []string, i int, idx *schema.Index) int {
	if i+1 < len(tokens) {
		idx.Comment = introspect.StripQuotes(tokens[i+1])
		return i + 1
	}
	return i
}

func parseKeyBlockSizeOption(tokens []string, i int, token string, idx *schema.Index) int {
	if token == "KEY_BLOCK_SIZE" {
		if i+1 < len(tokens) {
			idxToParse := i + 1
			if tokens[idxToParse] == "=" && idxToParse+1 < len(tokens) {
				idxToParse++
			} else if after, ok := strings.CutPrefix(tokens[idxToParse], "="); ok {
				tokens[idxToParse] = after
			}
			if val, err := strconv.Atoi(tokens[idxToParse]); err == nil {
				idx.KeyBlockSize = val
			}
			return idxToParse
		}
	} else {
		parts := strings.SplitN(tokens[i], "=", 2)
		if len(parts) == 2 {
			if val, err := strconv.Atoi(parts[1]); err == nil {
				idx.KeyBlockSize = val
			}
		}
	}
	return i
}

func parseWithParserOption(tokens []string, i int, idx *schema.Index) int {
	if i+2 < len(tokens) && strings.ToUpper(tokens[i+1]) == "PARSER" {
		idx.Parser = introspect.StripQuotes(tokens[i+2])
		return i + 2
	}
	return i
}

func parseIndexColumns(idx *schema.Index, colsStr string) {
	colsStr = strings.TrimPrefix(colsStr, "(")
	colsStr = strings.TrimSuffix(colsStr, ")")

	colTokens := introspect.SplitBy(colsStr, ',')
	for _, colToken := range colTokens {
		colToken = strings.TrimSpace(colToken)
		parts := introspect.Tokenize(colToken)
		if len(parts) > 0 {
			colIdx := parseColumnIndex(parts)
			idx.Columns = append(idx.Columns, colIdx)
		}
	}
}

func parseColumnIndex(parts []string) schema.ColumnIndex {
	colIdx := schema.ColumnIndex{}
	namePart := parts[0]

	if strings.HasPrefix(namePart, "(") && strings.HasSuffix(namePart, ")") {
		colIdx.Expression = strings.TrimSuffix(strings.TrimPrefix(namePart, "("), ")")
	} else {
		if parenIdx := strings.Index(namePart, "("); parenIdx != -1 {
			colIdx.Name = introspect.StripQuotes(namePart[:parenIdx])
			lenStr := strings.Trim(namePart[parenIdx:], "()")
			if l, err := strconv.Atoi(lenStr); err == nil {
				colIdx.Length = l
			}
		} else {
			colIdx.Name = introspect.StripQuotes(namePart)
		}
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

	return colIdx
}
