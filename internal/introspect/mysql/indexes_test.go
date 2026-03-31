package mysql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"smf/internal/schema"
)

func TestParseIndex_Basic(t *testing.T) {
	tests := []struct {
		name     string
		item     string
		expected *schema.Index
	}{
		{
			name: "simple key",
			item: "KEY `idx_name` (`name`)",
			expected: &schema.Index{
				Name: "idx_name",
				Columns: []schema.ColumnIndex{
					{Name: "name"},
				},
			},
		},
		{
			name: "index with length and asc sort",
			item: "KEY `idx` (`col1` (10) ASC)",
			expected: &schema.Index{
				Name: "idx",
				Columns: []schema.ColumnIndex{
					{Name: "col1", Length: 10, Order: schema.SortAsc},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idx, err := parseIndex(schema.DialectMySQL, tt.item)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, idx)
		})
	}
}

func TestParseIndex_Advanced(t *testing.T) {
	tests := []struct {
		name     string
		item     string
		expected *schema.Index
	}{
		{
			name: "unique key with using btree and visible",
			item: "UNIQUE KEY `idx_name` USING BTREE (`col1`(10) ASC, `col2` DESC) COMMENT 'hello' VISIBLE",
			expected: &schema.Index{
				Unique: true,
				Name:   "idx_name",
				Type:   schema.IndexTypeBTree,
				Columns: []schema.ColumnIndex{
					{Name: "col1", Length: 10, Order: schema.SortAsc},
					{Name: "col2", Order: schema.SortDesc},
				},
				Comment:    "hello",
				Visibility: schema.IndexVisible,
			},
		},
		{
			name: "index with invisible and comment",
			item: "INDEX `idx_comment` (`col1`) INVISIBLE COMMENT 'test'",
			expected: &schema.Index{
				Name: "idx_comment",
				Columns: []schema.ColumnIndex{
					{Name: "col1"},
				},
				Comment:    "test",
				Visibility: schema.IndexInvisible,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idx, err := parseIndex(schema.DialectMySQL, tt.item)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, idx)
		})
	}
}

func TestParseIndex_Special(t *testing.T) {
	tests := []struct {
		name     string
		item     string
		expected *schema.Index
	}{
		{
			name: "fulltext index",
			item: "FULLTEXT INDEX `ft_content` (`content`)",
			expected: &schema.Index{
				Name: "ft_content",
				Type: schema.IndexTypeFullText,
				Columns: []schema.ColumnIndex{
					{Name: "content"},
				},
			},
		},
		{
			name: "spatial index",
			item: "SPATIAL KEY `geom_idx` (`geom`)",
			expected: &schema.Index{
				Name: "geom_idx",
				Type: schema.IndexTypeSpatial,
				Columns: []schema.ColumnIndex{
					{Name: "geom"},
				},
			},
		},
		{
			name: "index with parser and key block size",
			item: "FULLTEXT INDEX `ft_idx` (`content`) WITH PARSER ngram KEY_BLOCK_SIZE=1024",
			expected: &schema.Index{
				Name: "ft_idx",
				Type: schema.IndexTypeFullText,
				Columns: []schema.ColumnIndex{
					{Name: "content"},
				},
				Parser:       "ngram",
				KeyBlockSize: 1024,
			},
		},
		{
			name: "index with key block size space separated",
			item: "INDEX `kb_idx` (`id`) KEY_BLOCK_SIZE 2048",
			expected: &schema.Index{
				Name: "kb_idx",
				Columns: []schema.ColumnIndex{
					{Name: "id"},
				},
				KeyBlockSize: 2048,
			},
		},
		{
			name: "functional index with expression",
			item: "INDEX `func_idx` ((ABS(`col`)))",
			expected: &schema.Index{
				Name: "func_idx",
				Columns: []schema.ColumnIndex{
					{Expression: "ABS(`col`)"},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idx, err := parseIndex(schema.DialectMySQL, tt.item)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, idx)
		})
	}
}

func TestParseIndex_MissingBranches(t *testing.T) {
	tests := []struct {
		name     string
		item     string
		expected *schema.Index
		wantErr  bool
	}{
		{
			name:    "empty declaration",
			item:    "",
			wantErr: true,
		},
		{
			name: "just unique",
			item: "UNIQUE",
			expected: &schema.Index{
				Unique: true,
			},
		},
		{
			name: "using option after columns",
			item: "INDEX `idx` (`col`) USING BTREE",
			expected: &schema.Index{
				Name: "idx",
				Type: schema.IndexTypeBTree,
				Columns: []schema.ColumnIndex{
					{Name: "col"},
				},
			},
		},
		{
			name: "using option without type",
			item: "INDEX `idx` (`col`) USING",
			expected: &schema.Index{
				Name: "idx",
				Columns: []schema.ColumnIndex{
					{Name: "col"},
				},
			},
		},
		{
			name: "comment without value",
			item: "INDEX `idx` (`col`) COMMENT",
			expected: &schema.Index{
				Name: "idx",
				Columns: []schema.ColumnIndex{
					{Name: "col"},
				},
			},
		},
		{
			name: "key block size with spaces around equals",
			item: "INDEX `idx` (`col`) KEY_BLOCK_SIZE = 1024",
			expected: &schema.Index{
				Name: "idx",
				Columns: []schema.ColumnIndex{
					{Name: "col"},
				},
				KeyBlockSize: 1024,
			},
		},
		{
			name: "key block size with spaces after equals",
			item: "INDEX `idx` (`col`) KEY_BLOCK_SIZE =1024",
			expected: &schema.Index{
				Name: "idx",
				Columns: []schema.ColumnIndex{
					{Name: "col"},
				},
				KeyBlockSize: 1024,
			},
		},
		{
			name: "key block size missing value",
			item: "INDEX `idx` (`col`) KEY_BLOCK_SIZE",
			expected: &schema.Index{
				Name: "idx",
				Columns: []schema.ColumnIndex{
					{Name: "col"},
				},
			},
		},
		{
			name: "key block size invalid value",
			item: "INDEX `idx` (`col`) KEY_BLOCK_SIZE=ABC",
			expected: &schema.Index{
				Name: "idx",
				Columns: []schema.ColumnIndex{
					{Name: "col"},
				},
			},
		},
		{
			name: "with parser missing parser",
			item: "INDEX `idx` (`col`) WITH PARSER",
			expected: &schema.Index{
				Name: "idx",
				Columns: []schema.ColumnIndex{
					{Name: "col"},
				},
			},
		},
		{
			name: "with missing parser keyword",
			item: "INDEX `idx` (`col`) WITH",
			expected: &schema.Index{
				Name: "idx",
				Columns: []schema.ColumnIndex{
					{Name: "col"},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idx, err := parseIndex(schema.DialectMySQL, tt.item)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, idx)
			}
		})
	}
}
