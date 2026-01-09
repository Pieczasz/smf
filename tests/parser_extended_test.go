package tests

import (
	"schemift/core"
	"schemift/parser"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExtendedTableOptions(t *testing.T) {
	p := parser.NewSQLParser()

	sql := `
CREATE TABLE extended_options (
    id INT
) 
STORAGE DISK
SECONDARY_ENGINE = NULL
TABLE_CHECKSUM = 1
UNION = (t1, t2)
ENGINE_ATTRIBUTE = '{"a":1}'
SECONDARY_ENGINE_ATTRIBUTE = '{"b":2}'
PAGE_COMPRESSED = 1
PAGE_COMPRESSION_LEVEL = 5
NODEGROUP = 123;
`
	db, err := p.ParseSchema(sql)
	require.NoError(t, err)
	tbl := db.FindTable("extended_options")
	require.NotNil(t, tbl)

	assert.Equal(t, "DISK", tbl.Options.StorageMedia)
	assert.Equal(t, "NULL", tbl.Options.MySQL.SecondaryEngine)
	assert.Equal(t, uint64(1), tbl.Options.MySQL.TableChecksum)
	assert.Equal(t, []string{"t1", "t2"}, tbl.Options.MySQL.Union)
	assert.Equal(t, `{"a":1}`, tbl.Options.MySQL.EngineAttribute)
	assert.Equal(t, `{"b":2}`, tbl.Options.MySQL.SecondaryEngineAttribute)
	assert.True(t, tbl.Options.MySQL.PageCompressed)
	assert.Equal(t, uint64(5), tbl.Options.MySQL.PageCompressionLevel)
	assert.Equal(t, uint64(123), tbl.Options.MySQL.Nodegroup)
}

func TestTiDBOptionsExtended(t *testing.T) {
	p := parser.NewSQLParser()
	sql := `
CREATE TABLE tidb_opts (
    id INT
)
SEQUENCE = 1
PLACEMENT POLICY = 'p1'
STATS_BUCKETS = 100
STATS_TOPN = 10
STATS_COL_LIST = 'c1, c2'
STATS_SAMPLE_RATE = 0.5;
`

	db, err := p.ParseSchema(sql)
	require.NoError(t, err)
	tbl := db.FindTable("tidb_opts")
	require.NotNil(t, tbl)

	assert.Equal(t, "p1", tbl.Options.TiDB.PlacementPolicy)
	assert.Equal(t, uint64(100), tbl.Options.TiDB.StatsBuckets)
	assert.Equal(t, uint64(10), tbl.Options.TiDB.StatsTopN)
	assert.Equal(t, "c1, c2", tbl.Options.TiDB.StatsColList)
	assert.Equal(t, 0.5, tbl.Options.TiDB.StatsSampleRate)
}

func TestColumnOptions(t *testing.T) {
	p := parser.NewSQLParser()

	sql := `
CREATE TABLE col_opts (
    c1 INT UNIQUE KEY,
    c2 INT COMMENT "my comment",
    c3 VARCHAR(100),
    c4 INT CHECK (c4 > 10),
    c5 INT REFERENCES other_table(id) ON DELETE CASCADE ON UPDATE SET NULL,
    c6 INT COLUMN_FORMAT FIXED,
    c7 INT STORAGE DISK,
    c8 BIGINT AUTO_RANDOM(5),
    c9 INT SECONDARY_ENGINE_ATTRIBUTE '{"x":1}'
);
`
	db, err := p.ParseSchema(sql)
	require.NoError(t, err)
	tbl := db.FindTable("col_opts")
	require.NotNil(t, tbl)

	require.Len(t, tbl.Constraints, 3)
	var uniq *core.Constraint
	for _, c := range tbl.Constraints {
		if c.Type == core.ConstraintUnique && len(c.Columns) == 1 && c.Columns[0] == "c1" {
			uniq = c
			break
		}
	}
	assert.NotNil(t, uniq, "Missing UNIQUE constraint for c1")

	c2 := tbl.FindColumn("c2")
	assert.Equal(t, "my comment", c2.Comment)

	var check *core.Constraint
	for _, c := range tbl.Constraints {
		if c.Type == core.ConstraintCheck && len(c.Columns) == 1 && c.Columns[0] == "c4" {
			check = c
			break
		}
	}
	assert.NotNil(t, check, "Missing CHECK constraint for c4")
	assert.Equal(t, "`c4`>10", check.CheckExpression)

	var fk *core.Constraint
	for _, c := range tbl.Constraints {
		if c.Type == core.ConstraintForeignKey && len(c.Columns) == 1 && c.Columns[0] == "c5" {
			fk = c
			break
		}
	}
	assert.NotNil(t, fk, "Missing FK constraint for c5")
	assert.Equal(t, "other_table", fk.ReferencedTable)
	assert.Equal(t, []string{"id"}, fk.ReferencedColumns)
	assert.Equal(t, core.RefActionCascade, fk.OnDelete)
	assert.Equal(t, core.RefActionSetNull, fk.OnUpdate)

	c6 := tbl.FindColumn("c6")
	assert.Equal(t, "FIXED", c6.ColumnFormat)

	c7 := tbl.FindColumn("c7")
	assert.Equal(t, "DISK", c7.Storage)

	c8 := tbl.FindColumn("c8")
	assert.Equal(t, uint64(5), c8.AutoRandom)

	c9 := tbl.FindColumn("c9")
	assert.Equal(t, `{"x":1}`, c9.SecondaryEngineAttribute)
}

func TestRowFormats(t *testing.T) {
	p := parser.NewSQLParser()
	formats := []struct {
		Format   string
		Expected string
	}{
		{"DEFAULT", "DEFAULT"},
		{"TOKUDB_LZMA", "TOKUDB_LZMA"},
		{"TOKUDB_SNAPPY", "TOKUDB_SNAPPY"},
	}

	for _, f := range formats {
		sql := "CREATE TABLE t_" + f.Format + " (id int) ROW_FORMAT=" + f.Format
		db, err := p.ParseSchema(sql)
		require.NoError(t, err)
		tbl := db.Tables[0]
		assert.Equal(t, f.Expected, tbl.Options.RowFormat)
	}
}
