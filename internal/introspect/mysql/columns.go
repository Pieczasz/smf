package mysql

import (
	"slices"
	"strings"

	"smf/internal/introspect"
	"smf/internal/schema"
)

var terminators = []string{"NOT", "NULL", "DEFAULT", "AUTO_INCREMENT", "PRIMARY",
	"KEY", "UNIQUE", "COMMENT", "COLLATE", "CHARSET", "REFERENCES",
	"CHECK", "ON", "CONSTRAINT", "INDEX", "GENERATED", "AS",
	"VISIBLE", "INVISIBLE", "COLUMN_FORMAT", "ENGINE_ATTRIBUTE",
	"SECONDARY_ENGINE_ATTRIBUTE", "STORAGE"}

// parseColumn parses a single column definition from a CREATE TABLE body item.
// Example input: "`id` bigint unsigned NOT NULL AUTO_INCREMENT".
func parseColumn(_ schema.Dialect, item string) (*schema.Column, error) {
	tokens := introspect.Tokenize(item)
	if len(tokens) == 0 {
		return nil, nil
	}

	name := schema.QuoteMySQLIdentifier(tokens[0])
	col := &schema.Column{
		Name:     name,
		Nullable: true,
	}

	var typeTokens []string
	var i int
	for i = 1; i < len(tokens); i++ {
		upperToken := strings.ToUpper(tokens[i])
		isTerm := slices.Contains(terminators, upperToken)
		if !isTerm {
			for _, term := range terminators {
				if strings.HasPrefix(upperToken, term+"=") {
					isTerm = true
					break
				}
			}
		}
		if isTerm {
			break
		}
		typeTokens = append(typeTokens, tokens[i])
	}

	rawType := strings.Join(typeTokens, " ")
	col.RawType = rawType
	col.Type = schema.NormalizeDataType(rawType)
	for ; i < len(tokens); i++ {
		upperToken := strings.ToUpper(tokens[i])
		i = applyColumnAttribute(col, tokens, i, upperToken)
	}
	return col, nil
}

// applyColumnAttribute processes a single column attribute keyword at position i and returns the new index.
func applyColumnAttribute(col *schema.Column, tokens []string, i int, upperToken string) int {
	if j := applyColumnNullability(col, tokens, i, upperToken); j != i {
		return j
	}
	if j := applyColumnKeyAttr(col, tokens, i, upperToken); j != i {
		return j
	}
	if j := applyColumnTextAttr(col, tokens, i, upperToken); j != i {
		return j
	}
	if j := applyColumnCheckAttr(col, tokens, i, upperToken); j != i {
		return j
	}
	if j := applyColumnGeneratedAttr(col, tokens, i, upperToken); j != i {
		return j
	}
	return applyColumnStorageAttr(col, tokens, i, upperToken)
}

func applyColumnGeneratedAttr(col *schema.Column, tokens []string, i int, upperToken string) int {
	switch upperToken {
	case "GENERATED":
		if i+1 < len(tokens) && strings.ToUpper(tokens[i+1]) == "ALWAYS" {
			i += 2
		} else {
			i++
		}
		if i < len(tokens) && strings.ToUpper(tokens[i]) == "AS" && i+1 < len(tokens) {
			col.IsGenerated = true
			col.GenerationExpression = strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(tokens[i+1], "("), ")"))
			return i + 2
		}
		return i
	case "AS":
		if i+1 < len(tokens) {
			col.IsGenerated = true
			col.GenerationExpression = strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(tokens[i+1], "("), ")"))
			return i + 2
		}
	case "VIRTUAL":
		col.GenerationStorage = schema.GenerationVirtual
		return i + 1
	case "STORED":
		col.GenerationStorage = schema.GenerationStored
		return i + 1
	case "VISIBLE":
		col.Invisible = false
		return i + 1
	case "INVISIBLE":
		col.Invisible = true
		return i + 1
	}
	return i
}

func applyColumnStorageAttr(col *schema.Column, tokens []string, i int, upperToken string) int {
	switch {
	case upperToken == "COLUMN_FORMAT":
		if i+1 < len(tokens) {
			if col.MySQL == nil {
				col.MySQL = &schema.MySQLColumnOptions{}
			}
			col.MySQL.ColumnFormat = strings.ToUpper(tokens[i+1])
			return i + 2
		}
	case upperToken == "ENGINE_ATTRIBUTE" || strings.HasPrefix(upperToken, "ENGINE_ATTRIBUTE="):
		if col.MySQL == nil {
			col.MySQL = &schema.MySQLColumnOptions{}
		}
		if upperToken == "ENGINE_ATTRIBUTE" {
			if i+1 < len(tokens) {
				idx := i + 1
				if tokens[idx] == "=" && idx+1 < len(tokens) {
					idx++
				} else if after, ok := strings.CutPrefix(tokens[idx], "="); ok {
					col.MySQL.PrimaryEngineAttribute = introspect.StripQuotes(after)
					return idx + 1
				}
				col.MySQL.PrimaryEngineAttribute = introspect.StripQuotes(tokens[idx])
				return idx + 1
			}
		} else {
			parts := strings.SplitN(tokens[i], "=", 2)
			if len(parts) == 2 {
				col.MySQL.PrimaryEngineAttribute = introspect.StripQuotes(parts[1])
			}
			return i + 1
		}
	case upperToken == "SECONDARY_ENGINE_ATTRIBUTE" || strings.HasPrefix(upperToken, "SECONDARY_ENGINE_ATTRIBUTE="):
		if col.MySQL == nil {
			col.MySQL = &schema.MySQLColumnOptions{}
		}
		if upperToken == "SECONDARY_ENGINE_ATTRIBUTE" {
			if i+1 < len(tokens) {
				idx := i + 1
				if tokens[idx] == "=" && idx+1 < len(tokens) {
					idx++
				} else if after, ok := strings.CutPrefix(tokens[idx], "="); ok {
					col.MySQL.SecondaryEngineAttribute = introspect.StripQuotes(after)
					return idx + 1
				}
				col.MySQL.SecondaryEngineAttribute = introspect.StripQuotes(tokens[idx])
				return idx + 1
			}
		} else {
			parts := strings.SplitN(tokens[i], "=", 2)
			if len(parts) == 2 {
				col.MySQL.SecondaryEngineAttribute = introspect.StripQuotes(parts[1])
			}
			return i + 1
		}
	case upperToken == "STORAGE":
		if i+1 < len(tokens) {
			if col.MySQL == nil {
				col.MySQL = &schema.MySQLColumnOptions{}
			}
			col.MySQL.Storage = strings.ToUpper(tokens[i+1])
			return i + 2
		}
	}
	return i
}

// applyColumnNullability handles NOT NULL / NULL / AUTO_INCREMENT.
func applyColumnNullability(col *schema.Column, tokens []string, i int, upperToken string) int {
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
func applyColumnKeyAttr(col *schema.Column, tokens []string, i int, upperToken string) int {
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
			col.DefaultValue = new(tokens[i+1])
			return i + 1
		}
	case "ON":
		return applyColumnOnClause(col, tokens, i)
	}
	return i
}

// applyColumnTextAttr handles COMMENT / COLLATE / CHARSET text options.
func applyColumnTextAttr(col *schema.Column, tokens []string, i int, upperToken string) int {
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
func applyColumnCheckAttr(col *schema.Column, tokens []string, i int, upperToken string) int {
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
func applyColumnOnClause(col *schema.Column, tokens []string, i int) int {
	if i+2 >= len(tokens) {
		return i
	}
	nextUpper := strings.ToUpper(tokens[i+1])
	action1 := strings.ToUpper(tokens[i+2])
	action, skip := resolveMultiWordAction(tokens, i, action1)

	switch nextUpper {
	case "DELETE":
		col.RefOnDelete = schema.ReferentialAction(action)
		return i + skip
	case "UPDATE":
		return applyOnUpdate(col, tokens, i, action, skip)
	}
	return i
}

// applyOnUpdate sets the ON UPDATE referential action or timestamp expression.
func applyOnUpdate(col *schema.Column, tokens []string, i int, action string, skip int) int {
	switch action {
	case "CASCADE", "RESTRICT", "SET NULL", "NO ACTION", "SET DEFAULT":
		col.RefOnUpdate = schema.ReferentialAction(action)
		return i + skip
	}
	col.OnUpdate = new(tokens[i+2])
	return i + 2
}

// applyColumnReferences handles the REFERENCES clause and returns the new index.
func applyColumnReferences(col *schema.Column, tokens []string, i int) int {
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
