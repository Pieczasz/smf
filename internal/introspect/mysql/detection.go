package mysql

import (
	"context"
	"database/sql"
	"strings"

	"smf/internal/core"
)

func detectDialect(ctx context.Context, db *sql.DB) (core.Dialect, string, error) {
	var varName, comment string

	err := db.QueryRowContext(ctx, "SHOW VARIABLES LIKE 'version_comment'").Scan(&varName, &comment)
	if err != nil {
		return "", "", err
	}

	comment = strings.ToLower(comment)

	switch {
	case strings.Contains(comment, "mariadb"):
		return core.DialectMariaDB, getVersion(ctx, db), nil
	case strings.Contains(comment, "tidb"):
		return core.DialectTiDB, getVersion(ctx, db), nil
	default:
		return core.DialectMySQL, getVersion(ctx, db), nil
	}
}

func getVersion(ctx context.Context, db *sql.DB) string {
	var version string
	_ = db.QueryRowContext(ctx, "SELECT VERSION()").Scan(&version)
	if idx := strings.Index(version, "-"); idx > 0 {
		version = version[:idx]
	}
	return version
}
