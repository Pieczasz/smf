package introspect

import "strings"

// Tokenize splits a SQL string by whitespace, but keeps whitespace intact if they
// are inside single quotes, double quotes, backticks, or parentheses.
//
// Example: "`id` bigint unsigned NOT NULL" -> ["`id`", "bigint", "unsigned", "NOT", "NULL"]
// Example: "ENUM('a','b')" -> ["ENUM('a','b')"].
func Tokenize(s string) []string {
	var tokens []string
	var current strings.Builder
	singular, doubled, backticked := false, false, false
	parenDepth := 0

	for _, r := range s {
		switch r {
		case '\'':
			if !doubled && !backticked {
				singular = !singular
			}
			current.WriteRune(r)
		case '"':
			if !singular && !backticked {
				doubled = !doubled
			}
			current.WriteRune(r)
		case '`':
			if !singular && !doubled {
				backticked = !backticked
			}
			current.WriteRune(r)
		case '(':
			if !singular && !doubled && !backticked {
				parenDepth++
			}
			current.WriteRune(r)
		case ')':
			if !singular && !doubled && !backticked {
				parenDepth--
			}
			current.WriteRune(r)
		case ' ', '\t':
			if !singular && !doubled && !backticked && parenDepth == 0 {
				if current.Len() > 0 {
					tokens = append(tokens, current.String())
					current.Reset()
				}
			} else {
				current.WriteRune(r)
			}
		default:
			current.WriteRune(r)
		}
	}

	if current.Len() > 0 {
		tokens = append(tokens, current.String())
	}
	return tokens
}

// FindMatchingParen returns the index of the closing parenthesis that matches
// the opening parenthesis at startPos. It respects quotes and nested parentheses.
func FindMatchingParen(s string, startPos int) (int, error) {
	if startPos >= len(s) || s[startPos] != '(' {
		return -1, nil
	}

	depth := 0
	singleQuoted, doubleQuoted, backticked := false, false, false

	for i := startPos; i < len(s); i++ {
		ch := s[i]

		if (singleQuoted || doubleQuoted) && ch == '\\' {
			i++
			continue
		}

		switch {
		case ch == '\'' && !doubleQuoted && !backticked:
			singleQuoted = !singleQuoted
		case ch == '"' && !singleQuoted && !backticked:
			doubleQuoted = !doubleQuoted
		case ch == '`' && !singleQuoted && !doubleQuoted:
			backticked = !backticked
		case !singleQuoted && !doubleQuoted && !backticked:
			switch ch {
			case '(':
				depth++
			case ')':
				depth--
				if depth == 0 {
					return i, nil
				}
			}
		}
	}

	return -1, nil
}

// SplitBy splits string s by delimiter when not inside quotes or parentheses.
// It returns a slice of trimmed substrings.
func SplitBy(s string, delimiter rune) []string {
	var parts []string
	var current strings.Builder
	singleQuoted, doubleQuoted, backticked := false, false, false
	parenDepth := 0

	for _, r := range s {
		switch r {
		case '\'':
			if !doubleQuoted && !backticked {
				singleQuoted = !singleQuoted
			}
			current.WriteRune(r)
		case '"':
			if !singleQuoted && !backticked {
				doubleQuoted = !doubleQuoted
			}
			current.WriteRune(r)
		case '`':
			if !singleQuoted && !doubleQuoted {
				backticked = !backticked
			}
			current.WriteRune(r)
		case '(':
			if !singleQuoted && !doubleQuoted && !backticked {
				parenDepth++
			}
			current.WriteRune(r)
		case ')':
			if !singleQuoted && !doubleQuoted && !backticked {
				parenDepth--
			}
			current.WriteRune(r)
		case delimiter:
			if !singleQuoted && !doubleQuoted && !backticked && parenDepth == 0 {
				if trimmed := strings.TrimSpace(current.String()); trimmed != "" {
					parts = append(parts, trimmed)
					current.Reset()
				}
			} else {
				current.WriteRune(r)
			}
		default:
			current.WriteRune(r)
		}
	}

	if last := strings.TrimSpace(current.String()); last != "" {
		parts = append(parts, last)
	}

	return parts
}
