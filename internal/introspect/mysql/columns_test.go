package mysql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"smf/internal/schema"
)

func TestParseColumn_Basic(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected *schema.Column
	}{
		{
			name:  "Simple Column",
			input: "`id` bigint unsigned NOT NULL AUTO_INCREMENT",
			expected: &schema.Column{
				Name:          "id",
				RawType:       "bigint unsigned",
				Type:          schema.DataTypeInt,
				Nullable:      false,
				AutoIncrement: true,
			},
		},
		{
			name:  "Nullable Column",
			input: "`desc` varchar(255) NULL",
			expected: &schema.Column{
				Name:     "desc",
				RawType:  "varchar(255)",
				Type:     schema.DataTypeString,
				Nullable: true,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseColumn(schema.DialectMySQL, tc.input)
			require.NoError(t, err)
			assert.Equal(t, tc.expected, got)
		})
	}
}

func TestParseColumn_Generated(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected *schema.Column
	}{
		{
			name:  "Generated Column STORED",
			input: "`calc` int GENERATED ALWAYS AS (a * b) STORED",
			expected: &schema.Column{
				Name:                 "calc",
				RawType:              "int",
				Type:                 schema.DataTypeInt,
				Nullable:             true,
				IsGenerated:          true,
				GenerationExpression: "a * b",
				GenerationStorage:    schema.GenerationStored,
			},
		},
		{
			name:  "Generated Column AS VIRTUAL",
			input: "`calc2` int AS (a + b) VIRTUAL",
			expected: &schema.Column{
				Name:                 "calc2",
				RawType:              "int",
				Type:                 schema.DataTypeInt,
				Nullable:             true,
				IsGenerated:          true,
				GenerationExpression: "a + b",
				GenerationStorage:    schema.GenerationVirtual,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseColumn(schema.DialectMySQL, tc.input)
			require.NoError(t, err)
			assert.Equal(t, tc.expected, got)
		})
	}
}

func TestParseColumn_StorageAndAttributes(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected *schema.Column
	}{
		{
			name:  "Invisible Column",
			input: "`secret` varchar(255) INVISIBLE",
			expected: &schema.Column{
				Name:      "secret",
				RawType:   "varchar(255)",
				Type:      schema.DataTypeString,
				Nullable:  true,
				Invisible: true,
			},
		},
		{
			name:  "Engine Attribute With Equals",
			input: "`data` varchar(100) ENGINE_ATTRIBUTE='{\"key\":\"value\"}'",
			expected: &schema.Column{
				Name:     "data",
				RawType:  "varchar(100)",
				Type:     schema.DataTypeString,
				Nullable: true,
				MySQL: &schema.MySQLColumnOptions{
					PrimaryEngineAttribute: `{"key":"value"}`,
				},
			},
		},
		{
			name:  "Engine Attribute Without Equals",
			input: "`data2` varchar(100) ENGINE_ATTRIBUTE '{\"key\":\"value\"}'",
			expected: &schema.Column{
				Name:     "data2",
				RawType:  "varchar(100)",
				Type:     schema.DataTypeString,
				Nullable: true,
				MySQL: &schema.MySQLColumnOptions{
					PrimaryEngineAttribute: `{"key":"value"}`,
				},
			},
		},
		{
			name:  "Column Format and Storage",
			input: "`data3` varchar(100) COLUMN_FORMAT FIXED STORAGE DISK",
			expected: &schema.Column{
				Name:     "data3",
				RawType:  "varchar(100)",
				Type:     schema.DataTypeString,
				Nullable: true,
				MySQL: &schema.MySQLColumnOptions{
					ColumnFormat: "FIXED",
					Storage:      "DISK",
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseColumn(schema.DialectMySQL, tc.input)
			require.NoError(t, err)
			assert.Equal(t, tc.expected, got)
		})
	}
}
