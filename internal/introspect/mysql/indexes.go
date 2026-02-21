package mysql

import (
	"database/sql"
	"strings"

	"smf/internal/core"
)

func introspectIndexes(ic *introspectCtx, t *core.Table) error {
	rows, err := ic.db.QueryContext(ic.ctx, `
		SELECT
			i.index_name,
			i.non_unique,
			i.index_type,
			i.comment,
			GROUP_CONCAT(CONCAT(c.column_name, IF(c.sub_part IS NULL, '', CONCAT('(', c.sub_part, ')'))) ORDER BY c.seq_in_index SEPARATOR ', ')
		FROM information_schema.statistics i
		JOIN information_schema.statistics c
			ON i.table_schema = c.table_schema
			AND i.table_name = c.table_name
			AND i.index_name = c.index_name
		WHERE i.table_schema = DATABASE() AND i.table_name = ?
		GROUP BY i.index_name, i.non_unique, i.index_type, i.comment
	`, t.Name)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var indexName, unique, indexType, comment, columns sql.NullString
		if err := rows.Scan(&indexName, &unique, &indexType, &comment, &columns); err != nil {
			return err
		}

		idx := &core.Index{
			Name:    indexName.String,
			Unique:  unique.String == "0",
			Type:    normalizeIndexType(indexType.String),
			Comment: comment.String,
		}

		for col := range strings.SplitSeq(columns.String, ", ") {
			idx.Columns = append(idx.Columns, core.ColumnIndex{Name: col})
		}

		t.Indexes = append(t.Indexes, idx)
	}

	return rows.Err()
}

func normalizeIndexType(t string) core.IndexType {
	// TODO: do we need to upper?
	switch strings.ToUpper(t) {
	case "BTREE":
		return core.IndexTypeBTree
	case "HASH":
		return core.IndexTypeHash
	case "FULLTEXT":
		return core.IndexTypeFullText
	case "SPATIAL":
		return core.IndexTypeSpatial
	default:
		return core.IndexTypeBTree
	}
}
