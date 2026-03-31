package mysql

import (
	"errors"
	"strings"

	"smf/internal/introspect"
	"smf/internal/schema"
)

// parseConstraint parses a table-level constraint from a CREATE TABLE body item.
//
// Handles: PRIMARY KEY, UNIQUE KEY, FOREIGN KEY, CHECK, and named CONSTRAINT declarations.
func parseConstraint(_ schema.Dialect, item string) (*schema.Constraint, error) {
	tokens := introspect.Tokenize(item)
	if len(tokens) == 0 {
		return nil, nil
	}

	c := &schema.Constraint{
		Enforced: new(true), // Default for check constraints
	}
	var i int

	i = parseConstraintName(c, tokens, i)

	if i >= len(tokens) {
		return nil, errors.New("incomplete constraint definition: " + item)
	}

	var err error
	_, err = dispatchConstraintType(c, tokens, i)
	if err != nil {
		return nil, err
	}

	if c.Type == "" {
		return nil, errors.New("failed to parse constraint, type is empty")
	}

	return c, nil
}

func parseConstraintName(c *schema.Constraint, tokens []string, i int) int {
	if strings.EqualFold(tokens[i], "CONSTRAINT") {
		i++
		if i < len(tokens) && !isConstraintType(tokens[i]) {
			c.Name = introspect.StripQuotes(tokens[i])
			i++
		}
	}
	return i
}

func dispatchConstraintType(c *schema.Constraint, tokens []string, i int) (int, error) {
	switch strings.ToUpper(tokens[i]) {
	case "PRIMARY":
		return parsePrimaryKey(c, tokens, i)
	case "UNIQUE":
		return parseUniqueKey(c, tokens, i), nil
	case "FOREIGN":
		return parseForeignKey(c, tokens, i)
	case "CHECK":
		return parseCheckConstraint(c, tokens, i), nil
	default:
		return i, errors.New("unknown constraint type: " + tokens[i])
	}
}

func isConstraintType(s string) bool {
	u := strings.ToUpper(s)
	return u == "PRIMARY" || u == "UNIQUE" || u == "FOREIGN" || u == "CHECK"
}

func parsePrimaryKey(c *schema.Constraint, tokens []string, i int) (int, error) {
	if i+1 >= len(tokens) || !strings.EqualFold(tokens[i+1], "KEY") {
		return i, errors.New("invalid primary key: " + tokens[i])
	}
	c.Type = schema.ConstraintPrimaryKey
	i += 2
	for i < len(tokens) && !strings.HasPrefix(tokens[i], "(") {
		i++
	}
	c.Columns, i = parseConstraintColumns(tokens, i)
	return i, nil
}

func parseUniqueKey(c *schema.Constraint, tokens []string, i int) int {
	c.Type = schema.ConstraintUnique
	i++
	if i < len(tokens) && (strings.EqualFold(tokens[i], "KEY") || strings.EqualFold(tokens[i], "INDEX")) {
		i++
	}
	for i < len(tokens) && !strings.HasPrefix(tokens[i], "(") {
		if c.Name == "" && !strings.EqualFold(tokens[i], "USING") {
			c.Name = introspect.StripQuotes(tokens[i])
		}
		i++
	}
	c.Columns, i = parseConstraintColumns(tokens, i)
	return i
}

func parseForeignKey(c *schema.Constraint, tokens []string, i int) (int, error) {
	if i+1 >= len(tokens) || !strings.EqualFold(tokens[i+1], "KEY") {
		return i, errors.New("invalid foreign key")
	}
	c.Type = schema.ConstraintForeignKey
	i += 2
	if i < len(tokens) && !strings.HasPrefix(tokens[i], "(") {
		if c.Name == "" {
			c.Name = introspect.StripQuotes(tokens[i])
		}
		i++
	}
	c.Columns, i = parseConstraintColumns(tokens, i)
	i = parseReferences(c, tokens, i)
	return i, nil
}

func parseCheckConstraint(c *schema.Constraint, tokens []string, i int) int {
	c.Type = schema.ConstraintCheck
	i++
	if i < len(tokens) && strings.HasPrefix(tokens[i], "(") {
		c.CheckExpression = strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(tokens[i], "("), ")"))
		i++
	}
	for i < len(tokens) {
		switch strings.ToUpper(tokens[i]) {
		case "NOT":
			if i+1 < len(tokens) && strings.EqualFold(tokens[i+1], "ENFORCED") {
				c.Enforced = new(false)
				i += 2
			} else {
				i++
			}
		case "ENFORCED":
			c.Enforced = new(true)
			i++
		default:
			i++
		}
	}
	return i
}

func parseConstraintColumns(tokens []string, i int) ([]string, int) {
	var cols []string
	if i < len(tokens) && strings.HasPrefix(tokens[i], "(") {
		colsStr := strings.TrimSuffix(strings.TrimPrefix(tokens[i], "("), ")")
		colTokens := introspect.SplitBy(colsStr, ',')
		for _, ct := range colTokens {
			parts := introspect.Tokenize(ct)
			if len(parts) > 0 {
				colName := parts[0]
				if parenIdx := strings.Index(colName, "("); parenIdx != -1 {
					colName = colName[:parenIdx]
				}
				cols = append(cols, introspect.StripQuotes(colName))
			}
		}
		i++
	}
	return cols, i
}

func parseReferences(c *schema.Constraint, tokens []string, i int) int {
	for i < len(tokens) {
		switch strings.ToUpper(tokens[i]) {
		case "REFERENCES":
			i = applyReferences(c, tokens, i)
		case "MATCH":
			i = applyMatch(c, tokens, i)
		case "ON":
			if i+2 < len(tokens) {
				i = parseOnAction(c, tokens, i)
			} else {
				i++
			}
		default:
			i++
		}
	}
	return i
}

func applyReferences(c *schema.Constraint, tokens []string, i int) int {
	i++
	if i < len(tokens) {
		c.ReferencedTable = introspect.StripQuotes(tokens[i])
		i++
	}
	c.ReferencedColumns, i = parseConstraintColumns(tokens, i)
	return i
}

// TODO: maybe remove this?
func applyMatch(c *schema.Constraint, tokens []string, i int) int {
	if i+1 < len(tokens) {
		matchType := strings.ToUpper(tokens[i+1])
		if matchType == "FULL" || matchType == "PARTIAL" || matchType == "SIMPLE" {
			c.Match = matchType
			return i + 2
		}
	}
	return i + 1
}

func parseOnAction(c *schema.Constraint, tokens []string, i int) int {
	actionType := strings.ToUpper(tokens[i+1]) // DELETE or UPDATE
	i += 2

	var action schema.ReferentialAction
	action, i = getReferentialAction(tokens, i)

	if action != "" {
		switch actionType {
		case "DELETE":
			c.OnDelete = action
		case "UPDATE":
			c.OnUpdate = action
		}
	}
	return i
}

func getReferentialAction(tokens []string, i int) (schema.ReferentialAction, int) {
	switch strings.ToUpper(tokens[i]) {
	case "RESTRICT":
		i++
		return schema.RefActionRestrict, i
	case "CASCADE":
		i++
		return schema.RefActionCascade, i
	case "SET":
		return getSetReferentialAction(tokens, i)
	case "NO":
		return getNoReferentialAction(tokens, i)
	default:
		i++ // Skip unknown
		return "", i
	}
}

func getSetReferentialAction(tokens []string, i int) (schema.ReferentialAction, int) {
	if i+1 < len(tokens) {
		sub := strings.ToUpper(tokens[i+1])
		i += 2
		switch sub {
		case "NULL":
			return schema.RefActionSetNull, i
		case "DEFAULT":
			return schema.RefActionSetDefault, i
		}
	} else {
		i++
	}
	return "", i
}

func getNoReferentialAction(tokens []string, i int) (schema.ReferentialAction, int) {
	if i+1 < len(tokens) && strings.EqualFold(tokens[i+1], "ACTION") {
		i += 2
		return schema.RefActionNoAction, i
	}
	i++
	return "", i
}
