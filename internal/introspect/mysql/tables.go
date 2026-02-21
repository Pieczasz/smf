package mysql

import (
	"database/sql"
	"strings"

	"smf/internal/core"
)

func introspectTables(ic *introspectCtx, db *core.Database) error {
	rows, err := ic.db.QueryContext(ic.ctx, `
		SELECT table_name, table_comment
		FROM information_schema.tables
		WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE'
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var name, comment string
		if err := rows.Scan(&name, &comment); err != nil {
			return err
		}

		t := &core.Table{
			Name:    name,
			Comment: comment,
			Options: core.TableOptions{},
		}

		if err := introspectTableOptions(ic, t); err != nil {
			return err
		}

		if err := introspectColumns(ic, t); err != nil {
			return err
		}

		if err := introspectIndexes(ic, t); err != nil {
			return err
		}

		// if err := introspectConstraints(introspectCtx); err != nil {
		// return err
		// }

		db.Tables = append(db.Tables, t)
	}

	return rows.Err()
}
func introspectTableOptions(i *introspectCtx, t *core.Table) error {
	row := i.db.QueryRowContext(i.ctx, `
		SELECT engine, table_collation, auto_increment
		FROM information_schema.tables
		WHERE table_schema = DATABASE() AND table_name = ?
	`, t.Name)

	var engine, collate string
	var autoIncrement sql.NullInt64
	if err := row.Scan(&engine, &collate, &autoIncrement); err != nil {
		return err
	}

	charset := ""
	if idx := strings.Index(collate, "_"); idx > 0 {
		charset = collate[:idx]
		collate = collate[idx+1:]
	}

	t.Options.MySQL = &core.MySQLTableOptions{
		Engine:        engine,
		Charset:       charset,
		Collate:       collate,
		AutoIncrement: uint64(autoIncrement.Int64),
	}

	return nil
}
