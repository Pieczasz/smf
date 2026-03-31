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

func TestParseColumn_KeyAndTextAttrs(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected *schema.Column
	}{
		{
			name:  "Primary Key",
			input: "`id` int PRIMARY KEY",
			expected: &schema.Column{
				Name:       "id",
				RawType:    "int",
				Type:       schema.DataTypeInt,
				Nullable:   true,
				PrimaryKey: true,
			},
		},
		{
			name:  "Unique Key",
			input: "`email` varchar(255) UNIQUE KEY",
			expected: &schema.Column{
				Name:     "email",
				RawType:  "varchar(255)",
				Type:     schema.DataTypeString,
				Nullable: true,
				Unique:   true,
			},
		},
		{
			name:  "Default Value",
			input: "`status` varchar(50) DEFAULT 'active'",
			expected: &schema.Column{
				Name:         "status",
				RawType:      "varchar(50)",
				Type:         schema.DataTypeString,
				Nullable:     true,
				DefaultValue: new("'active'"),
			},
		},
		{
			name:  "Comment and Collate",
			input: "`name` varchar(255) COLLATE 'utf8mb4_bin' COMMENT 'user name'",
			expected: &schema.Column{
				Name:     "name",
				RawType:  "varchar(255)",
				Type:     schema.DataTypeString,
				Nullable: true,
				Collate:  "'utf8mb4_bin'",
				Comment:  "'user name'",
			},
		},
		{
			name:  "Character Set",
			input: "`desc` text CHARACTER SET 'utf8mb4'",
			expected: &schema.Column{
				Name:     "desc",
				RawType:  "text",
				Type:     schema.DataTypeString,
				Nullable: true,
				Charset:  "'utf8mb4'",
			},
		},
		{
			name:  "Charset alias",
			input: "`desc2` text CHARSET 'utf8mb4'",
			expected: &schema.Column{
				Name:     "desc2",
				RawType:  "text",
				Type:     schema.DataTypeString,
				Nullable: true,
				Charset:  "'utf8mb4'",
			},
		},
		{
			name:  "Check constraint",
			input: "`age` int CHECK (`age` > 0)",
			expected: &schema.Column{
				Name:     "age",
				RawType:  "int",
				Type:     schema.DataTypeInt,
				Nullable: true,
				Check:    "(`age` > 0)",
			},
		},
		{
			name:  "References",
			input: "`user_id` int REFERENCES `users` (`id`) ON DELETE CASCADE ON UPDATE SET NULL",
			expected: &schema.Column{
				Name:        "user_id",
				RawType:     "int",
				Type:        schema.DataTypeInt,
				Nullable:    true,
				References:  "`users`(`id`)",
				RefOnDelete: schema.RefActionCascade,
				RefOnUpdate: schema.RefActionSetNull,
			},
		},
		{
			name:  "On Update Current Timestamp",
			input: "`updated_at` timestamp ON UPDATE CURRENT_TIMESTAMP",
			expected: &schema.Column{
				Name:     "updated_at",
				RawType:  "timestamp",
				Type:     schema.DataTypeDatetime,
				Nullable: true,
				OnUpdate: new("CURRENT_TIMESTAMP"),
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

func TestParseColumn_MissingBranches(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected *schema.Column
	}{
		{
			name:  "Visible Column",
			input: "`id` int VISIBLE",
			expected: &schema.Column{
				Name:      "id",
				RawType:   "int",
				Type:      schema.DataTypeInt,
				Nullable:  true,
				Invisible: false,
			},
		},
		{
			name:  "Generated without ALWAYS",
			input: "`calc` int GENERATED AS (a * b)",
			expected: &schema.Column{
				Name:                 "calc",
				RawType:              "int",
				Type:                 schema.DataTypeInt,
				Nullable:             true,
				IsGenerated:          true,
				GenerationExpression: "a * b",
			},
		},
		{
			name:  "Generated missing expression",
			input: "`calc` int GENERATED AS",
			expected: &schema.Column{
				Name:     "calc",
				RawType:  "int",
				Type:     schema.DataTypeInt,
				Nullable: true,
			},
		},
		{
			name:  "Generated AS missing expression",
			input: "`calc` int AS",
			expected: &schema.Column{
				Name:     "calc",
				RawType:  "int",
				Type:     schema.DataTypeInt,
				Nullable: true,
			},
		},
		{
			name:  "Secondary Engine Attribute With Equals",
			input: "`data` varchar(100) SECONDARY_ENGINE_ATTRIBUTE='{\"key\":\"value\"}'",
			expected: &schema.Column{
				Name:     "data",
				RawType:  "varchar(100)",
				Type:     schema.DataTypeString,
				Nullable: true,
				MySQL: &schema.MySQLColumnOptions{
					SecondaryEngineAttribute: `{"key":"value"}`,
				},
			},
		},
		{
			name:  "Secondary Engine Attribute Without Equals",
			input: "`data2` varchar(100) SECONDARY_ENGINE_ATTRIBUTE '{\"key\":\"value\"}'",
			expected: &schema.Column{
				Name:     "data2",
				RawType:  "varchar(100)",
				Type:     schema.DataTypeString,
				Nullable: true,
				MySQL: &schema.MySQLColumnOptions{
					SecondaryEngineAttribute: `{"key":"value"}`,
				},
			},
		},
		{
			name:  "Primary Engine Attribute With Spaces",
			input: "`data3` varchar(100) ENGINE_ATTRIBUTE = '{\"key\":\"value\"}'",
			expected: &schema.Column{
				Name:     "data3",
				RawType:  "varchar(100)",
				Type:     schema.DataTypeString,
				Nullable: true,
				MySQL: &schema.MySQLColumnOptions{
					PrimaryEngineAttribute: `{"key":"value"}`,
				},
			},
		},
		{
			name:  "Secondary Engine Attribute With Spaces",
			input: "`data4` varchar(100) SECONDARY_ENGINE_ATTRIBUTE = '{\"key\":\"value\"}'",
			expected: &schema.Column{
				Name:     "data4",
				RawType:  "varchar(100)",
				Type:     schema.DataTypeString,
				Nullable: true,
				MySQL: &schema.MySQLColumnOptions{
					SecondaryEngineAttribute: `{"key":"value"}`,
				},
			},
		},
		{
			name:  "Primary Key without KEY keyword",
			input: "`id` int PRIMARY",
			expected: &schema.Column{
				Name:     "id",
				RawType:  "int",
				Type:     schema.DataTypeInt,
				Nullable: true,
			},
		},
		{
			name:  "Unique without KEY keyword",
			input: "`email` varchar(255) UNIQUE",
			expected: &schema.Column{
				Name:     "email",
				RawType:  "varchar(255)",
				Type:     schema.DataTypeString,
				Nullable: true,
				Unique:   true,
			},
		},
		{
			name:  "ON clause without DELETE or UPDATE",
			input: "`updated_at` timestamp ON",
			expected: &schema.Column{
				Name:     "updated_at",
				RawType:  "timestamp",
				Type:     schema.DataTypeDatetime,
				Nullable: true,
			},
		},
		{
			name:  "ON clause without anything",
			input: "`updated_at` timestamp ON UPDATE",
			expected: &schema.Column{
				Name:     "updated_at",
				RawType:  "timestamp",
				Type:     schema.DataTypeDatetime,
				Nullable: true,
			},
		},
		{
			name:  "REFERENCES clause without ref",
			input: "`user_id` int REFERENCES",
			expected: &schema.Column{
				Name:     "user_id",
				RawType:  "int",
				Type:     schema.DataTypeInt,
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
