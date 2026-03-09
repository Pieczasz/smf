package mysql

import (
	"slices"
	"strings"

	"smf/internal/core"
	"smf/internal/introspect"
)

var terminators = []string{"NOT", "NULL", "DEFAULT", "AUTO_INCREMENT", "PRIMARY",
	"KEY", "UNIQUE", "COMMENT", "COLLATE", "CHARSET", "REFERENCES",
	"CHECK", "ON", "CONSTRAINT", "INDEX", "KEY"}

// parseColumn parses a single column definition from a CREATE TABLE body item.
// Example input: "`id` bigint unsigned NOT NULL AUTO_INCREMENT".
func parseColumn(_ core.Dialect, item string) (*core.Column, error) {
	tokens := introspect.Tokenize(item)
	if len(tokens) == 0 {
		return nil, nil
	}

	name := core.QuoteMySQLIdentifier(tokens[0])
	col := &core.Column{
		Name:     name,
		Nullable: true,
	}

	var typeTokens []string
	var i int
	for i = 1; i < len(tokens); i++ {
		upperToken := strings.ToUpper(tokens[i])
		if slices.Contains(terminators, upperToken) {
			break
		}
		typeTokens = append(typeTokens, tokens[i])
	}

	rawType := strings.Join(typeTokens, " ")
	col.RawType = rawType
	col.Type = core.NormalizeDataType(rawType)
	for ; i < len(tokens); i++ {
		upperToken := strings.ToUpper(tokens[i])
		i = applyColumnAttribute(col, tokens, i, upperToken)
	}
	return col, nil
}

// applyColumnAttribute processes a single column attribute keyword at position i and returns the new index.
func applyColumnAttribute(col *core.Column, tokens []string, i int, upperToken string) int {
	if j := applyColumnNullability(col, tokens, i, upperToken); j != i {
		return j
	}
	if j := applyColumnKeyAttr(col, tokens, i, upperToken); j != i {
		return j
	}
	if j := applyColumnTextAttr(col, tokens, i, upperToken); j != i {
		return j
	}
	return applyColumnCheckAttr(col, tokens, i, upperToken)
}

// applyColumnNullability handles NOT NULL / NULL / AUTO_INCREMENT.
func applyColumnNullability(col *core.Column, tokens []string, i int, upperToken string) int {
	switch upperToken {
	case "NOT":
		if i+1 < len(tokens) && strings.ToUpper(tokens[i+1]) == "NULL" {
			col.Nullable = false
			return i + 1
		}
	case "NULL":
		col.Nullable = true
		return i
	case "AUTO_INCREMENT":
		col.AutoIncrement = true
		return i
	}
	return i
}

// applyColumnKeyAttr handles PRIMARY KEY / UNIQUE / DEFAULT / ON clauses.
func applyColumnKeyAttr(col *core.Column, tokens []string, i int, upperToken string) int {
	switch upperToken {
	case "PRIMARY":
		if i+1 < len(tokens) && strings.ToUpper(tokens[i+1]) == "KEY" {
			col.PrimaryKey = true
			return i + 1
		}
	case "UNIQUE":
		col.Unique = true
		if i+1 < len(tokens) && strings.ToUpper(tokens[i+1]) == "KEY" {
			return i + 1
		}
		return i
	case "DEFAULT":
		if i+1 < len(tokens) {
			val := tokens[i+1]
			col.DefaultValue = &val
			return i + 1
		}
	case "ON":
		return applyColumnOnClause(col, tokens, i)
	}
	return i
}

// applyColumnTextAttr handles COMMENT / COLLATE / CHARSET text options.
func applyColumnTextAttr(col *core.Column, tokens []string, i int, upperToken string) int {
	switch upperToken {
	case "COMMENT":
		if i+1 < len(tokens) {
			col.Comment = tokens[i+1]
			return i + 1
		}
	case "COLLATE":
		if i+1 < len(tokens) {
			col.Collate = tokens[i+1]
			return i + 1
		}
	case "CHARSET":
		if i+1 < len(tokens) {
			col.Charset = tokens[i+1]
			return i + 1
		}
	}
	return i
}

// applyColumnCheckAttr handles CHARACTER SET / CHECK / REFERENCES.
func applyColumnCheckAttr(col *core.Column, tokens []string, i int, upperToken string) int {
	switch upperToken {
	case "CHARACTER":
		if i+2 < len(tokens) && strings.ToUpper(tokens[i+1]) == "SET" {
			col.Charset = tokens[i+2]
			return i + 2
		}
	case "CHECK":
		if i+1 < len(tokens) {
			col.Check = tokens[i+1]
			return i + 1
		}
	case "REFERENCES":
		return applyColumnReferences(col, tokens, i)
	}
	return i
}

// resolveMultiWordAction resolves a referential action that may span two tokens (e.g. "SET NULL", "NO ACTION").
func resolveMultiWordAction(tokens []string, i int, action1 string) (string, int) {
	if (action1 == "SET" || action1 == "NO") && i+3 < len(tokens) {
		return action1 + " " + strings.ToUpper(tokens[i+3]), 3
	}
	return action1, 2
}

// applyColumnOnClause handles ON DELETE / ON UPDATE clauses and returns the new index.
func applyColumnOnClause(col *core.Column, tokens []string, i int) int {
	if i+2 >= len(tokens) {
		return i
	}
	nextUpper := strings.ToUpper(tokens[i+1])
	action1 := strings.ToUpper(tokens[i+2])
	action, skip := resolveMultiWordAction(tokens, i, action1)

	switch nextUpper {
	case "DELETE":
		col.RefOnDelete = core.ReferentialAction(action)
		return i + skip
	case "UPDATE":
		return applyOnUpdate(col, tokens, i, action, skip)
	}
	return i
}

// applyOnUpdate sets the ON UPDATE referential action or timestamp expression.
func applyOnUpdate(col *core.Column, tokens []string, i int, action string, skip int) int {
	switch action {
	case "CASCADE", "RESTRICT", "SET NULL", "NO ACTION", "SET DEFAULT":
		col.RefOnUpdate = core.ReferentialAction(action)
		return i + skip
	}
	val := tokens[i+2]
	col.OnUpdate = &val
	return i + 2
}

// applyColumnReferences handles REFERENCES clause and returns the new index.
func applyColumnReferences(col *core.Column, tokens []string, i int) int {
	if i+1 >= len(tokens) {
		return i
	}
	ref := tokens[i+1]
	i++
	if i+1 < len(tokens) && strings.HasPrefix(tokens[i+1], "(") {
		ref += tokens[i+1]
		i++
	}
	col.References = ref
	return i
}
