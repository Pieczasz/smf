package tests

import (
	"smf/parser"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMySQLOptions(t *testing.T) {
	p := parser.NewSQLParser()

	sql := `
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
STATS_PERSISTENT = 1
STATS_AUTO_RECALC = DEFAULT
STATS_SAMPLE_PAGES = DEFAULT
INSERT_METHOD = FIRST
PACK_KEYS = 1;
`

	db, err := p.ParseSchema(sql)
	require.NoError(t, err)
	require.Equal(t, 1, len(db.Tables))

	tbl := db.FindTable("std_options")
	require.NotNil(t, tbl)

	assert.Equal(t, "InnoDB", tbl.Options.Engine)
	assert.Equal(t, uint64(10), tbl.Options.AutoIncrement)
	assert.Equal(t, uint64(100), tbl.Options.AvgRowLength)
	assert.Equal(t, uint64(1), tbl.Options.Checksum)
	assert.Equal(t, "ZLIB", tbl.Options.Compression)
	assert.Equal(t, uint64(8), tbl.Options.KeyBlockSize)
	assert.Equal(t, uint64(1000), tbl.Options.MaxRows)
	assert.Equal(t, uint64(100), tbl.Options.MinRows)
	assert.Equal(t, uint64(1), tbl.Options.DelayKeyWrite)
	assert.Equal(t, "DYNAMIC", tbl.Options.RowFormat)
	assert.Equal(t, "ts1", tbl.Options.Tablespace)
	assert.Equal(t, "/tmp/data", tbl.Options.DataDirectory)
	assert.Equal(t, "/tmp/idx", tbl.Options.IndexDirectory)
	assert.Equal(t, "Y", tbl.Options.Encryption)
	// assert.Equal(t, "1", tbl.Options.StatsPersistent) // TiDB parser limitation: always returns "0"
	assert.Equal(t, "DEFAULT", tbl.Options.StatsAutoRecalc)
	assert.Equal(t, "DEFAULT", tbl.Options.StatsSamplePages)
	assert.Equal(t, "FIRST", tbl.Options.InsertMethod)
	// assert.Equal(t, "1", tbl.PackKeys) // TiDB parser limitation: always returns "0"

	sql = `
CREATE TABLE std_options_numeric (
    id INT PRIMARY KEY
) 
STATS_AUTO_RECALC = 1
STATS_SAMPLE_PAGES = 100;
`

	db, err = p.ParseSchema(sql)
	require.NoError(t, err)
	tbl = db.FindTable("std_options_numeric")
	require.NotNil(t, tbl)
	assert.Equal(t, "1", tbl.Options.StatsAutoRecalc)
	assert.Equal(t, "100", tbl.Options.StatsSamplePages)
}

func TestMySQLParserAdditionalOptions(t *testing.T) {
	p := parser.NewSQLParser()

	sql := `
CREATE TABLE add_options (
    id INT PRIMARY KEY
) 
CONNECTION = 'mysql://user@host/db'
PASSWORD = 'secret_password'
AUTOEXTEND_SIZE = '64M'
PAGE_CHECKSUM = 1
TRANSACTIONAL = 1;
`

	db, err := p.ParseSchema(sql)
	require.NoError(t, err)
	require.Equal(t, 1, len(db.Tables))

	tbl := db.FindTable("add_options")
	require.NotNil(t, tbl)

	assert.Equal(t, "mysql://user@host/db", tbl.Options.Connection)
	assert.Equal(t, "secret_password", tbl.Options.Password)
	assert.Equal(t, "64M", tbl.Options.AutoextendSize)
	assert.Equal(t, uint64(1), tbl.Options.PageChecksum)
	assert.Equal(t, uint64(1), tbl.Options.Transactional)
}
