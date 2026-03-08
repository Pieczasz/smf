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

		switch upperToken {
		case "NOT":
			if i+1 < len(tokens) && strings.ToUpper(tokens[i+1]) == "NULL" {
				col.Nullable = false
				i++
			}
		case "NULL":
			col.Nullable = true
		case "AUTO_INCREMENT":
			col.AutoIncrement = true
		case "PRIMARY":
			if i+1 < len(tokens) && strings.ToUpper(tokens[i+1]) == "KEY" {
				col.PrimaryKey = true
				i++
			}
		case "UNIQUE":
			col.Unique = true
			// Optional: sometimes written as UNIQUE KEY
			if i+1 < len(tokens) && strings.ToUpper(tokens[i+1]) == "KEY" {
				i++
			}
		case "DEFAULT":
			if i+1 < len(tokens) {
				val := tokens[i+1]
				col.DefaultValue = &val
				i++
			}
		case "ON":
			if i+2 < len(tokens) {
				nextUpper := strings.ToUpper(tokens[i+1])
				action1 := strings.ToUpper(tokens[i+2])

				parseAction := func() (string, int) {
					if action1 == "SET" || action1 == "NO" {
						if i+3 < len(tokens) {
							action2 := strings.ToUpper(tokens[i+3])
							return action1 + " " + action2, 3
						}
					}
					return action1, 2
				}

				switch nextUpper {
				case "DELETE":
					action, skip := parseAction()
					col.RefOnDelete = core.ReferentialAction(action)
					i += skip

				case "UPDATE":
					action, skip := parseAction()
					if action == "CASCADE" || action == "RESTRICT" || action == "SET NULL" || action == "NO ACTION" || action == "SET DEFAULT" {
						col.RefOnUpdate = core.ReferentialAction(action)
						i += skip
					} else {
						val := tokens[i+2]
						col.OnUpdate = &val
						i += 2
					}
				}
			}
		case "COMMENT":
			if i+1 < len(tokens) {
				col.Comment = tokens[i+1]
				i++
			}
		case "COLLATE":
			if i+1 < len(tokens) {
				col.Collate = tokens[i+1]
				i++
			}
		case "CHARSET":
			if i+1 < len(tokens) {
				col.Charset = tokens[i+1]
				i++
			}
		case "CHARACTER":
			if i+2 < len(tokens) && strings.ToUpper(tokens[i+1]) == "SET" {
				col.Charset = tokens[i+2]
				i += 2
			}
		case "CHECK":
			if i+1 < len(tokens) {
				col.Check = tokens[i+1]
				i++
			}
		case "REFERENCES":
			if i+1 < len(tokens) {
				ref := tokens[i+1]
				i++
				if i+1 < len(tokens) && strings.HasPrefix(tokens[i+1], "(") {
					ref += tokens[i+1]
					i++
				}
				col.References = ref
			}
		}
	}

	return col, nil
}
