package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"smf/internal/schema"
)

var (
	ErrUnsupportedMySQLVersion   = errors.New("unsupported MySQL version: minimum required is 8.0.23")
	ErrUnsupportedMariaDBVersion = errors.New("unsupported MariaDB version: minimum required is 10.3.4")
	ErrUnsupportedTiDBVersion    = errors.New("unsupported TiDB version: minimum required is 5.3.0")
)

func detectDialect(ctx context.Context, db *sql.DB) (schema.Dialect, string, error) {
	var varName, comment string

	err := db.QueryRowContext(ctx, "SHOW VARIABLES LIKE 'version_comment'").Scan(&varName, &comment)
	if err != nil {
		return "", "", err
	}

	comment = strings.ToLower(comment)

	version, err := getVersion(ctx, db)
	if err != nil {
		return "", "", err
	}

	var dialect schema.Dialect
	switch {
	case strings.Contains(comment, "mariadb"):
		dialect = schema.DialectMariaDB
		if err := checkMariaDBVersion(version); err != nil {
			return "", "", err
		}
	case strings.Contains(comment, "tidb"):
		dialect = schema.DialectTiDB
		if err := checkTiDBVersion(version); err != nil {
			return "", "", err
		}
	default:
		dialect = schema.DialectMySQL
		if err := checkMySQLVersion(version); err != nil {
			return "", "", err
		}
	}

	return dialect, version, nil
}

func getVersion(ctx context.Context, db *sql.DB) (string, error) {
	var version string
	err := db.QueryRowContext(ctx, "SELECT VERSION()").Scan(&version)
	if err != nil {
		return "", err
	}
	if version, _, found := strings.Cut(version, "-"); found {
		return version, nil
	}
	return version, nil
}

func parseVersion(version string) (int, int, int, error) {
	parts := strings.Split(version, ".")
	if len(parts) < 3 {
		return 0, 0, 0, fmt.Errorf("invalid version format: %s", version)
	}
	major, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, 0, 0, err
	}
	minor, err := strconv.Atoi(parts[1])
	if err != nil {
		return 0, 0, 0, err
	}
	patch, err := strconv.Atoi(parts[2])
	if err != nil {
		return 0, 0, 0, err
	}
	return major, minor, patch, nil
}

func checkMySQLVersion(version string) error {
	major, minor, patch, err := parseVersion(version)
	if err != nil {
		return err
	}
	if major < 8 || (major == 8 && minor == 0 && patch < 23) {
		return ErrUnsupportedMySQLVersion
	}
	return nil
}

func checkMariaDBVersion(version string) error {
	major, minor, patch, err := parseVersion(version)
	if err != nil {
		return err
	}
	if major < 10 || (major == 10 && minor < 3) || (major == 10 && minor == 3 && patch < 4) {
		return ErrUnsupportedMariaDBVersion
	}
	return nil
}

func checkTiDBVersion(version string) error {
	major, minor, _, err := parseVersion(version)
	if err != nil {
		return err
	}
	if major < 5 || (major == 5 && minor < 3) {
		return ErrUnsupportedTiDBVersion
	}
	return nil
}
