package diff

import (
	"fmt"
	"os"
	"smf/internal/parser"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDiffFull(t *testing.T) {
	oldSQL := `
CREATE TABLE std_options (
    id INT PRIMARY KEY
)
ENGINE = InnoDB
AUTO_INCREMENT = 10
AVG_ROW_LENGTH = 100
CHECKSUM = 1
COMPRESSION = 'ZLIB'
KEY_BLOCK_SIZE = 8
MAX_ROWS = 1000
MIN_ROWS = 100
DELAY_KEY_WRITE = 1
ROW_FORMAT = DYNAMIC
TABLESPACE = ts1
DATA DIRECTORY = '/tmp/data'
INDEX DIRECTORY = '/tmp/idx'
ENCRYPTION = 'Y'
STATS_AUTO_RECALC = DEFAULT
STATS_SAMPLE_PAGES = DEFAULT
INSERT_METHOD = FIRST;

CREATE TABLE std_options_numeric (
    id INT PRIMARY KEY
)
STATS_AUTO_RECALC = 1
STATS_SAMPLE_PAGES = 100;

CREATE TABLE add_options (
    id INT PRIMARY KEY
)
CONNECTION = 'mysql://user@host/db'
PASSWORD = 'secret_password'
AUTOEXTEND_SIZE = '64M'
PAGE_CHECKSUM = 1
TRANSACTIONAL = 1;

CREATE TABLE all_features (
    id INT AUTO_INCREMENT,
    t_tinyint TINYINT NOT NULL,
    t_varchar VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT 'default_val',
    t_text TEXT,
    t_enum ENUM('small', 'medium', 'large') DEFAULT 'medium',
    t_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    t_json JSON,
    g_col INT GENERATED ALWAYS AS (id + 1) VIRTUAL,
    g_col_stored INT GENERATED ALWAYS AS (id * 2) STORED,

    PRIMARY KEY (id),
    UNIQUE KEY idx_unique_varchar (t_varchar),
    KEY idx_regular (t_tinyint),
    CONSTRAINT chk_positive CHECK (id > 0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Comprehensive table';

CREATE TABLE related_features (
    id INT PRIMARY KEY,
    f_id INT,
    CONSTRAINT fk_related FOREIGN KEY (f_id) REFERENCES all_features(id) ON DELETE CASCADE ON UPDATE RESTRICT
);`

	newSQL := `
CREATE TABLE std_options (
    id INT PRIMARY KEY
)
ENGINE = MyISAM
AUTO_INCREMENT = 20
AVG_ROW_LENGTH = 200
CHECKSUM = 2
COMPRESSION = 'LZ4'
KEY_BLOCK_SIZE = 16
MAX_ROWS = 2000
MIN_ROWS = 50
DELAY_KEY_WRITE = 0
ROW_FORMAT = COMPACT
TABLESPACE = ts2
DATA DIRECTORY = '/tmp/data2'
INDEX DIRECTORY = '/tmp/idx2'
ENCRYPTION = 'N'
STATS_AUTO_RECALC = 1
STATS_SAMPLE_PAGES = 100
INSERT_METHOD = LAST;

CREATE TABLE std_options_numeric (
    id INT PRIMARY KEY
)
STATS_AUTO_RECALC = DEFAULT
STATS_SAMPLE_PAGES = 200;

CREATE TABLE add_options (
    id INT PRIMARY KEY
)
CONNECTION = 'mysql://user@host/db2'
PASSWORD = 'new_secret'
AUTOEXTEND_SIZE = '128M'
PAGE_CHECKSUM = 2
TRANSACTIONAL = 0;

CREATE TABLE all_features (
    id INT AUTO_INCREMENT,
    t_tinyint TINYINT NULL,
    t_varchar VARCHAR(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT 'changed',
    t_enum ENUM('small', 'medium', 'large') DEFAULT 'large',
    t_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    t_json JSON,
    new_col INT,
    g_col INT GENERATED ALWAYS AS (id + 2) VIRTUAL,
    g_col_stored INT GENERATED ALWAYS AS (id * 3) STORED,

    PRIMARY KEY (id),
	UNIQUE KEY idx_unique_varchar (t_enum),
	KEY idx_regular (t_enum),
    CONSTRAINT chk_positive CHECK (id >= 0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Comprehensive table v2';

CREATE TABLE related_features (
    id INT PRIMARY KEY,
    f_id INT,
    CONSTRAINT fk_related FOREIGN KEY (f_id) REFERENCES all_features(id) ON DELETE RESTRICT ON UPDATE RESTRICT
);`

	p := parser.NewSQLParser()
	oldDB, err := p.ParseSchema(oldSQL)
	require.NoError(t, err)
	newDB, err := p.ParseSchema(newSQL)
	require.NoError(t, err)

	d := Diff(oldDB, newDB)
	require.NotNil(t, d)

	assert.Empty(t, d.AddedTables)
	assert.Empty(t, d.RemovedTables)
	require.GreaterOrEqual(t, len(d.ModifiedTables), 4)

	std := findTableDiff(t, d, "std_options")
	assertOptionChange(t, std, "ENGINE")
	assertOptionChange(t, std, "AUTO_INCREMENT")
	assertOptionChange(t, std, "AVG_ROW_LENGTH")
	assertOptionChange(t, std, "CHECKSUM")
	assertOptionChange(t, std, "COMPRESSION")
	assertOptionChange(t, std, "KEY_BLOCK_SIZE")
	assertOptionChange(t, std, "MAX_ROWS")
	assertOptionChange(t, std, "MIN_ROWS")
	assertOptionChange(t, std, "DELAY_KEY_WRITE")
	assertOptionChange(t, std, "ROW_FORMAT")
	assertOptionChange(t, std, "TABLESPACE")
	assertOptionChange(t, std, "DATA DIRECTORY")
	assertOptionChange(t, std, "INDEX DIRECTORY")
	assertOptionChange(t, std, "ENCRYPTION")
	assertOptionChange(t, std, "STATS_AUTO_RECALC")
	assertOptionChange(t, std, "STATS_SAMPLE_PAGES")
	assertOptionChange(t, std, "INSERT_METHOD")

	stdNum := findTableDiff(t, d, "std_options_numeric")
	assertOptionChange(t, stdNum, "STATS_AUTO_RECALC")
	assertOptionChange(t, stdNum, "STATS_SAMPLE_PAGES")

	add := findTableDiff(t, d, "add_options")
	assertOptionChange(t, add, "CONNECTION")
	assertOptionChange(t, add, "PASSWORD")
	assertOptionChange(t, add, "AUTOEXTEND_SIZE")
	assertOptionChange(t, add, "PAGE_CHECKSUM")
	assertOptionChange(t, add, "TRANSACTIONAL")

	af := findTableDiff(t, d, "all_features")
	assertOptionChange(t, af, "COMMENT")

	assert.True(t, hasColumnChange(af, "t_tinyint"))
	assert.True(t, hasColumnChange(af, "t_varchar"))
	assert.True(t, hasColumnChange(af, "t_enum"))
	assert.True(t, hasColumnChange(af, "t_timestamp"))
	assert.True(t, hasColumnChange(af, "g_col"))
	assert.True(t, hasColumnChange(af, "g_col_stored"))
	assert.True(t, hasAddedColumn(af, "new_col"))
	assert.True(t, hasRemovedColumn(af, "t_text"))

	assert.True(t, hasModifiedConstraint(af, "chk_positive"))
	assert.True(t, hasModifiedConstraint(af, "idx_unique_varchar"))
	assert.True(t, hasModifiedIndex(af, "idx_regular"))

	rf := findTableDiff(t, d, "related_features")
	assert.True(t, hasModifiedConstraint(rf, "fk_related"))

	out := d.String()
	assert.Contains(t, out, "Schema differences")
	assert.Contains(t, out, "Options changed")
	assert.Contains(t, out, "Modified columns")
	assert.Contains(t, out, "t_tinyint")
	assert.Contains(t, out, "nullable")
	assert.Contains(t, out, "t_varchar")
	assert.Contains(t, out, "default")
	assert.Contains(t, out, "on_update")
	assert.Contains(t, out, "Modified constraints")
	assert.Contains(t, out, "chk_positive")
	assert.Contains(t, out, "check_expression")
	assert.Contains(t, out, "Modified indexes")
	assert.Contains(t, out, "idx_regular")
	assert.Contains(t, out, "columns")

	f, err := os.CreateTemp("", "smf-diff-full-*.txt")
	require.NoError(t, err)
	defer func(name string) {
		err := os.Remove(name)
		if err != nil {
			fmt.Println(err)
		}
	}(f.Name())
	require.NoError(t, f.Close())

	require.NoError(t, d.SaveToFile(f.Name()))
	b, err := os.ReadFile(f.Name())
	require.NoError(t, err)
	assert.Contains(t, string(b), "Schema differences")
}

func findTableDiff(t *testing.T, d *SchemaDiff, name string) *TableDiff {
	t.Helper()
	for _, td := range d.ModifiedTables {
		if td != nil && td.Name == name {
			return td
		}
	}
	require.FailNow(t, "expected modified table diff not found", "table=%s", name)
	return nil
}

func assertOptionChange(t *testing.T, td *TableDiff, option string) {
	t.Helper()
	for _, ch := range td.ModifiedOptions {
		if ch != nil && ch.Name == option {
			assert.NotEqual(t, ch.Old, ch.New)
			return
		}
	}
	require.FailNow(t, "expected option change not found", "table=%s option=%s", td.Name, option)
}

func hasColumnChange(td *TableDiff, col string) bool {
	for _, ch := range td.ModifiedColumns {
		if ch != nil && ch.Name == col {
			return true
		}
	}
	return false
}

func hasAddedColumn(td *TableDiff, col string) bool {
	for _, c := range td.AddedColumns {
		if c != nil && c.Name == col {
			return true
		}
	}
	return false
}

func hasRemovedColumn(td *TableDiff, col string) bool {
	for _, c := range td.RemovedColumns {
		if c != nil && c.Name == col {
			return true
		}
	}
	return false
}

func hasModifiedConstraint(td *TableDiff, constraintName string) bool {
	for _, ch := range td.ModifiedConstraints {
		if ch != nil && ch.Name == constraintName {
			return true
		}
	}
	return false
}

func hasModifiedIndex(td *TableDiff, indexName string) bool {
	for _, ch := range td.ModifiedIndexes {
		if ch != nil && ch.Name == indexName {
			return true
		}
	}
	return false
}
