package mysql

import (
	"database/sql"
	"strings"

	"smf/internal/core"
)

func introspectColumns(ic *introspectCtx, t *core.Table) error {
	rows, err := ic.db.QueryContext(ic.ctx, `
		SELECT
			c.column_name,
			c.column_type,
			c.column_comment,
			c.is_nullable,
			c.column_default,
			c.extra,
			c.character_set_name,
			c.collation_name,
			c.column_key,
			c.generation_expression
		FROM information_schema.columns c
		WHERE c.table_schema = DATABASE() AND c.table_name = ?
		ORDER BY c.ordinal_position
	`, t.Name)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		col := new(core.Column)
		// TODO: how to do this easier
		var name, colType, comment, nullable, defaultVal, extra, charset, collation, colKey, genExpr sql.NullString
		if err := rows.Scan(&name, &colType, &comment, &nullable, &defaultVal, &extra, &charset, &collation, &colKey, &genExpr); err != nil {
			return err
		}

		isPK := colKey.String == "PRI"
		isAutoInc := strings.Contains(extra.String, "auto_increment")

		col = &core.Column{
			Name:          name.String,
			RawType:       colType.String,
			Type:          core.NormalizeDataType(colType.String),
			Nullable:      nullable.String == "YES",
			PrimaryKey:    isPK,
			AutoIncrement: isAutoInc,
			Comment:       comment.String,
			Charset:       charset.String,
			Collate:       strings.ReplaceAll(collation.String, charset.String+"_", ""),
		}

		if defaultVal.Valid {
			col.DefaultValue = &defaultVal.String
		}

		if genExpr.Valid {
			col.IsGenerated = true
			col.GenerationExpression = genExpr.String
			col.GenerationStorage = core.GenerationStored
		}

		t.Columns = append(t.Columns, col)
	}

	return rows.Err()
}
