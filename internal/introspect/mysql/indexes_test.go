package mysql

import (
	"reflect"
	"testing"

	"smf/internal/schema"
)

func TestParseIndex(t *testing.T) {
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
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(idx, tt.expected) {
				t.Errorf("\ngot:      %#v\nexpected: %#v", idx, tt.expected)
			}
		})
	}
}
func TestParseIndexMore(t *testing.T) {
	item := "KEY `idx` (`col1` (10) ASC)"
	idx, _ := parseIndex(schema.DialectMySQL, item)
	if idx.Columns[0].Length != 10 {
		t.Errorf("Expected length 10, got %d", idx.Columns[0].Length)
	}
}
