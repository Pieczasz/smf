package mysql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQuoteIdentifier(t *testing.T) {
	gen := NewMySQLGenerator()
	require.NotNil(t, gen)

	tests := []struct {
		name     string
		input    string
		expected string
		desc     string
	}{
		{
			name:     "simple_identifier",
			input:    "users",
			expected: "`users`",
			desc:     "simple identifier without special chars",
		},
		{
			name:     "identifier_with_spaces",
			input:    "user table",
			expected: "`user table`",
			desc:     "identifier with spaces should be preserved",
		},
		{
			name:     "identifier_with_backticks",
			input:    "user`table",
			expected: "`user``table`",
			desc:     "backticks should be doubled (escaped)",
		},
		{
			name:     "identifier_with_multiple_backticks",
			input:    "tab`le`name",
			expected: "`tab``le``name`",
			desc:     "multiple backticks should all be doubled",
		},
		{
			name:     "identifier_with_trailing_spaces",
			input:    "  users  ",
			expected: "`users`",
			desc:     "leading/trailing spaces should be trimmed",
		},
		{
			name:     "identifier_with_numbers",
			input:    "user123",
			expected: "`user123`",
			desc:     "identifier with numbers",
		},
		{
			name:     "identifier_with_underscore",
			input:    "user_data",
			expected: "`user_data`",
			desc:     "identifier with underscores",
		},
		{
			name:     "mysql_keyword",
			input:    "select",
			expected: "`select`",
			desc:     "MySQL keywords should still be quoted",
		},
		{
			name:     "identifier_with_hyphen",
			input:    "user-data",
			expected: "`user-data`",
			desc:     "identifier with hyphens",
		},
		{
			name:     "identifier_with_unicode",
			input:    "用户表",
			expected: "`用户表`",
			desc:     "identifier with unicode characters",
		},
		{
			name:     "empty_string",
			input:    "",
			expected: "``",
			desc:     "empty string should result in empty quoted identifier",
		},
		{
			name:     "only_spaces",
			input:    "   ",
			expected: "``",
			desc:     "only spaces should trim to empty, then quote",
		},
		{
			name:     "identifier_at_max_mysql_length",
			input:    "a",
			expected: "`a`",
			desc:     "minimum length identifier",
		},
		{
			name:     "complex_identifier",
			input:    "`schema`.`table`",
			expected: "```schema``.``table```",
			desc:     "complex identifier with backticks and dots",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := gen.QuoteIdentifier(tt.input)
			assert.Equal(t, tt.expected, result, tt.desc)
		})
	}
}

func TestQuoteIdentifierEdgeCases(t *testing.T) {
	gen := NewMySQLGenerator()

	tests := []struct {
		name  string
		input string
		check func(t *testing.T, result string)
		desc  string
	}{
		{
			name:  "result_is_quoted",
			input: "test",
			check: func(t *testing.T, result string) {
				assert.True(t, len(result) > 0, "result should not be empty")
				assert.Equal(t, '`', rune(result[0]), "result should start with backtick")
				assert.Equal(t, '`', rune(result[len(result)-1]), "result should end with backtick")
			},
			desc: "all quoted identifiers should be wrapped in backticks",
		},
		{
			name:  "preserves_content_structure",
			input: "my_table",
			check: func(t *testing.T, result string) {
				assert.Equal(t, "`my_table`", result)
			},
			desc: "quoted identifier should preserve the content",
		},
		{
			name:  "idempotency_is_not_required",
			input: "already`quoted",
			check: func(t *testing.T, result string) {
				result2 := gen.QuoteIdentifier(result)
				assert.NotEqual(t, result, result2, "quoting again produces different result (backticks get escaped again)")
			},
			desc: "quoting is not idempotent due to backtick escaping",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := gen.QuoteIdentifier(tt.input)
			tt.check(t, result)
		})
	}
}

func TestQuoteString(t *testing.T) {
	gen := NewMySQLGenerator()
	require.NotNil(t, gen)

	tests := []struct {
		name     string
		input    string
		expected string
		desc     string
	}{
		{
			name:     "simple_string",
			input:    "hello",
			expected: "'hello'",
			desc:     "simple string",
		},
		{
			name:     "single_quote",
			input:    "it's",
			expected: "'it''s'",
			desc:     "single quote should be doubled (MySQL standard)",
		},
		{
			name:     "backslash",
			input:    "back\\slash",
			expected: "'back\\\\slash'",
			desc:     "backslash should be doubled",
		},
		{
			name:     "null_byte",
			input:    "null\x00byte",
			expected: "'null\\0byte'",
			desc:     "null byte should be escaped as \\0",
		},
		{
			name:     "newline",
			input:    "line1\nline2",
			expected: "'line1\\nline2'",
			desc:     "newline should be escaped as \\n",
		},
		{
			name:     "carriage_return",
			input:    "return\rcarriage",
			expected: "'return\\rcarriage'",
			desc:     "carriage return should be escaped as \\r",
		},
		{
			name:     "ctrl_z",
			input:    "ctrl\x1Az",
			expected: "'ctrl\\Zz'",
			desc:     "Ctrl+Z should be escaped as \\Z",
		},
		{
			name:     "empty_string",
			input:    "",
			expected: "''",
			desc:     "empty string",
		},
		{
			name:     "multiple_special_chars",
			input:    "line1\nline2\rline3\\end",
			expected: "'line1\\nline2\\rline3\\\\end'",
			desc:     "multiple special characters",
		},
		{
			name:     "only_special_chars",
			input:    "'\\\n\r\x00\x1A",
			expected: "'''\\\\\\n\\r\\0\\Z'",
			desc:     "only special characters",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := gen.QuoteString(tt.input)
			assert.Equal(t, tt.expected, result, tt.desc)
		})
	}
}

func TestQuoteIdentifierAndStringIntegration(t *testing.T) {
	gen := NewMySQLGenerator()

	tableName := "user_data"
	stringValue := "John's data"

	quotedTable := gen.QuoteIdentifier(tableName)
	quotedString := gen.QuoteString(stringValue)

	assert.Equal(t, "`user_data`", quotedTable)
	assert.Equal(t, "'John''s data'", quotedString)

	statement := "INSERT INTO " + quotedTable + " (name) VALUES (" + quotedString + ");"
	expected := "INSERT INTO `user_data` (name) VALUES ('John''s data');"
	assert.Equal(t, expected, statement)
}
