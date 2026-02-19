package mysql

import (
	"database/sql"
	"strings"

	"smf/internal/core"
)

func DetectDialect(db *sql.DB) (core.Dialect, string, error) {
	var varName, comment string

	err := db.QueryRow("SHOW VARIABLES LIKE 'version_comment'").Scan(&varName, &comment)
	if err != nil {
		return "", "", err
	}

	comment = strings.ToLower(comment)

	switch {
	case strings.Contains(comment, "mariadb"):
		return core.DialectMariaDB, getVersion(db), nil
	case strings.Contains(comment, "tidb"):
		return core.DialectTiDB, getVersion(db), nil
	default:
		return core.DialectMySQL, getVersion(db), nil
	}
}

func getVersion(db *sql.DB) string {
	var version string
	_ = db.QueryRow("SELECT VERSION()").Scan(&version)
	if idx := strings.Index(version, "-"); idx > 0 {
		version = version[:idx]
	}
	return version
}
