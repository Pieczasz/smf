package introspect

import (
	"strings"
)

// quoteState tracks nesting inside string literals and parentheses during tokenization.
type quoteState struct {
	singular   bool
	doubled    bool
	backticked bool
	parenDepth int
}

// Tokenize splits a SQL string by whitespace, but keeps whitespace intact if they
// are inside single quotes, double quotes, backticks, or parentheses.
//
// Example: "`id` bigint unsigned NOT NULL" -> ["`id`", "bigint", "unsigned", "NOT", "NULL"]
// Example: "ENUM('a','b')" -> ["ENUM('a','b')"].
func Tokenize(s string) []string {
	var tokens []string
	var current strings.Builder
	var q quoteState

	for _, r := range s {
		if r == ' ' || r == '\t' {
			if !q.nested() {
				if current.Len() > 0 {
					tokens = append(tokens, current.String())
					current.Reset()
				}
				continue
			}
		}
		q.updateQuotes(r)
		q.updateParens(r)
		current.WriteRune(r)
	}

	if current.Len() > 0 {
		tokens = append(tokens, current.String())
	}
	return tokens
}

func (q *quoteState) nested() bool {
	return q.singular || q.doubled || q.backticked || q.parenDepth > 0
}

func (q *quoteState) updateQuotes(r rune) {
	switch r {
	case '\'':
		if !q.doubled && !q.backticked {
			q.singular = !q.singular
		}
	case '"':
		if !q.singular && !q.backticked {
			q.doubled = !q.doubled
		}
	case '`':
		if !q.singular && !q.doubled {
			q.backticked = !q.backticked
		}
	}
}

func (q *quoteState) updateParens(r rune) {
	if q.singular || q.doubled || q.backticked {
		return
	}
	switch r {
	case '(':
		q.parenDepth++
	case ')':
		q.parenDepth--
	}
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

		i = applyParenState(ch, i, &depth, &singleQuoted, &doubleQuoted, &backticked)
		if depth == 0 && i >= startPos+1 {
			return i, nil
		}
	}

	return -1, nil
}

// applyParenState updates quote/depth state for a single character and returns the (possibly unchanged) index.
func applyParenState(ch byte, i int, depth *int, singleQuoted, doubleQuoted, backticked *bool) int {
	toggleParenQuotes(ch, singleQuoted, doubleQuoted, backticked)
	if !*singleQuoted && !*doubleQuoted && !*backticked {
		updateParenDepth(ch, depth)
	}
	return i
}

func toggleParenQuotes(ch byte, singleQuoted, doubleQuoted, backticked *bool) {
	switch ch {
	case '\'':
		if !*doubleQuoted && !*backticked {
			*singleQuoted = !*singleQuoted
		}
	case '"':
		if !*singleQuoted && !*backticked {
			*doubleQuoted = !*doubleQuoted
		}
	case '`':
		if !*singleQuoted && !*doubleQuoted {
			*backticked = !*backticked
		}
	}
}

func updateParenDepth(ch byte, depth *int) {
	switch ch {
	case '(':
		*depth++
	case ')':
		*depth--
	}
}

// SplitBy splits string s by delimiter when not inside quotes or parentheses.
// It returns a slice of trimmed substrings.
func SplitBy(s string, delimiter rune) []string {
	var parts []string
	var current strings.Builder
	var q quoteState

	for _, r := range s {
		if r == delimiter && !q.nested() {
			if trimmed := strings.TrimSpace(current.String()); trimmed != "" {
				parts = append(parts, trimmed)
				current.Reset()
			}
		} else {
			current.WriteRune(r)
		}
	}

	if last := strings.TrimSpace(current.String()); last != "" {
		parts = append(parts, last)
	}

	return parts
}
