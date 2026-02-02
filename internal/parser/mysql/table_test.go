package mysql

import (
	"testing"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/stretchr/testify/assert"

	"smf/internal/core"
)

func TestParseTableOptionsStandard(t *testing.T) {
	sql := `CREATE TABLE t (id INT)
		ENGINE=InnoDB
		DEFAULT CHARSET=utf8mb4
		COLLATE=utf8mb4_bin
		AUTO_INCREMENT=100
		ROW_FORMAT=DYNAMIC
		COMMENT='note';`

	table := parseSingleTable(t, sql)
	assert.Equal(t, "InnoDB", table.Options.Engine)
	assert.Equal(t, "utf8mb4", table.Options.Charset)
	assert.Equal(t, "utf8mb4_bin", table.Options.Collate)
	assert.Equal(t, uint64(100), table.Options.AutoIncrement)
	assert.Equal(t, "DYNAMIC", table.Options.RowFormat)
	assert.Equal(t, "note", table.Comment)
}

func TestParseTableOptionsStats(t *testing.T) {
	sql := `CREATE TABLE t (id INT)
		STATS_PERSISTENT=0
		STATS_AUTO_RECALC=1
		STATS_SAMPLE_PAGES=128;`

	table := parseSingleTable(t, sql)
	opts := table.Options
	assert.Equal(t, "0", opts.StatsPersistent)
	assert.Equal(t, "1", opts.StatsAutoRecalc)
	assert.Equal(t, "128", opts.StatsSamplePages)
}

func TestApplyPackKeysDefaultOption(t *testing.T) {
	table := &core.Table{}
	applyStandardTableOption(&ast.TableOption{Tp: ast.TableOptionPackKeys, Default: true}, table)
	assert.Equal(t, "DEFAULT", table.Options.PackKeys)
}

func TestApplyPackKeysNumericOption(t *testing.T) {
	table := &core.Table{}
	applyStandardTableOption(&ast.TableOption{Tp: ast.TableOptionPackKeys, UintValue: 1}, table)
	assert.Equal(t, "1", table.Options.PackKeys)
}

func TestApplyMySQLTableOptionAndTruthyValues(t *testing.T) {
	table := &core.Table{}

	applyMySQLTableOption(&ast.TableOption{Tp: ast.TableOptionSecondaryEngine, StrValue: "InnoDB"}, table)
	assert.Equal(t, "InnoDB", table.Options.MySQL.SecondaryEngine)

	applyMySQLTableOption(&ast.TableOption{Tp: ast.TableOptionSecondaryEngineNull}, table)
	assert.Equal(t, "NULL", table.Options.MySQL.SecondaryEngine)

	applyMySQLTableOption(&ast.TableOption{Tp: ast.TableOptionTableCheckSum, UintValue: 1}, table)
	assert.Equal(t, uint64(1), table.Options.MySQL.TableChecksum)

	applyMySQLTableOption(&ast.TableOption{Tp: ast.TableOptionEngineAttribute, StrValue: "engine_attr"}, table)
	assert.Equal(t, "engine_attr", table.Options.MySQL.EngineAttribute)

	applyMySQLTableOption(&ast.TableOption{Tp: ast.TableOptionSecondaryEngineAttribute, StrValue: "secondary_attr"}, table)
	assert.Equal(t, "secondary_attr", table.Options.MySQL.SecondaryEngineAttribute)

	applyMySQLTableOption(&ast.TableOption{Tp: ast.TableOptionPageCompressed, StrValue: "ON"}, table)
	assert.True(t, table.Options.MySQL.PageCompressed)

	applyMySQLTableOption(&ast.TableOption{Tp: ast.TableOptionPageCompressionLevel, UintValue: 5}, table)
	assert.Equal(t, uint64(5), table.Options.MySQL.PageCompressionLevel)

	applyMySQLTableOption(&ast.TableOption{Tp: ast.TableOptionIetfQuotes, BoolValue: true}, table)
	assert.True(t, table.Options.MySQL.IetfQuotes)

	applyMySQLTableOption(&ast.TableOption{Tp: ast.TableOptionNodegroup, UintValue: 3}, table)
	assert.Equal(t, uint64(3), table.Options.MySQL.Nodegroup)
}

func TestOptionTruthyAndRowFormatToString(t *testing.T) {
	assert.True(t, optionTruthy(true, "", 0))
	assert.True(t, optionTruthy(false, "", 1))
	assert.True(t, optionTruthy(false, "ON", 0))
	assert.False(t, optionTruthy(false, "OFF", 0))

	assert.Equal(t, "DYNAMIC", rowFormatToString(ast.RowFormatDynamic))
	assert.Equal(t, "", rowFormatToString(9999))
}

func TestParseTiDBStatsSampleRateOption(t *testing.T) {
	sql := `CREATE TABLE t (id INT) STATS_SAMPLE_RATE=0.25;`

	table := parseSingleTable(t, sql)
	assert.InDelta(t, 0.25, table.Options.TiDB.StatsSampleRate, 0.0001)
}

func TestTableOptionHelpers(t *testing.T) {
	table := &core.Table{}

	applyStandardTableOption(&ast.TableOption{Tp: ast.TableOptionAvgRowLength, UintValue: 10}, table)
	assert.Equal(t, uint64(10), table.Options.AvgRowLength)

	applyStandardTableOption(&ast.TableOption{Tp: ast.TableOptionKeyBlockSize, UintValue: 8}, table)
	assert.Equal(t, uint64(8), table.Options.KeyBlockSize)

	applyStandardTableOption(&ast.TableOption{Tp: ast.TableOptionMaxRows, UintValue: 100}, table)
	assert.Equal(t, uint64(100), table.Options.MaxRows)

	applyStandardTableOption(&ast.TableOption{Tp: ast.TableOptionMinRows, UintValue: 1}, table)
	assert.Equal(t, uint64(1), table.Options.MinRows)

	applyStandardTableOption(&ast.TableOption{Tp: ast.TableOptionDelayKeyWrite, UintValue: 1}, table)
	assert.Equal(t, uint64(1), table.Options.DelayKeyWrite)

	applyStandardTableOption(&ast.TableOption{Tp: ast.TableOptionTablespace, StrValue: "ts"}, table)
	assert.Equal(t, "ts", table.Options.Tablespace)

	applyStandardTableOption(&ast.TableOption{Tp: ast.TableOptionDataDirectory, StrValue: "/data"}, table)
	assert.Equal(t, "/data", table.Options.DataDirectory)

	applyStandardTableOption(&ast.TableOption{Tp: ast.TableOptionIndexDirectory, StrValue: "/idx"}, table)
	assert.Equal(t, "/idx", table.Options.IndexDirectory)

	applyStandardTableOption(&ast.TableOption{Tp: ast.TableOptionEncryption, StrValue: "Y"}, table)
	assert.Equal(t, "Y", table.Options.Encryption)

	applyStandardTableOption(&ast.TableOption{Tp: ast.TableOptionStorageMedia, StrValue: "SSD"}, table)
	assert.Equal(t, "SSD", table.Options.StorageMedia)

	applyStandardTableOption(&ast.TableOption{Tp: ast.TableOptionInsertMethod, StrValue: "FIRST"}, table)
	assert.Equal(t, "FIRST", table.Options.InsertMethod)

	applyStandardTableOption(&ast.TableOption{Tp: ast.TableOptionConnection, StrValue: "conn"}, table)
	assert.Equal(t, "conn", table.Options.Connection)

	applyStandardTableOption(&ast.TableOption{Tp: ast.TableOptionPassword, StrValue: "pwd"}, table)
	assert.Equal(t, "pwd", table.Options.Password)

	applyStandardTableOption(&ast.TableOption{Tp: ast.TableOptionAutoextendSize, StrValue: "64M"}, table)
	assert.Equal(t, "64M", table.Options.AutoextendSize)

	applyStandardTableOption(&ast.TableOption{Tp: ast.TableOptionPageChecksum, UintValue: 1}, table)
	assert.Equal(t, uint64(1), table.Options.PageChecksum)

	applyStandardTableOption(&ast.TableOption{Tp: ast.TableOptionTransactional, UintValue: 1}, table)
	assert.Equal(t, uint64(1), table.Options.Transactional)

	assert.False(t, applyStandardTableOption(&ast.TableOption{Tp: ast.TableOptionShardRowID, UintValue: 1}, table))

	applyStatsPersistentOption(&ast.TableOption{Default: true}, table)
	applyStatsAutoRecalcOption(&ast.TableOption{Default: true}, table)
	applyStatsSamplePagesOption(&ast.TableOption{Default: true}, table)
	assert.Equal(t, "DEFAULT", table.Options.StatsPersistent)
	assert.Equal(t, "DEFAULT", table.Options.StatsAutoRecalc)
	assert.Equal(t, "DEFAULT", table.Options.StatsSamplePages)

	assert.False(t, applyMySQLTableOption(&ast.TableOption{Tp: ast.TableOptionEngine}, table))

	applyUnionOption(&ast.TableOption{TableNames: []*ast.TableName{{Name: ast.NewCIStr("t1")}, {Name: ast.NewCIStr("t2")}}}, table)
	assert.Equal(t, []string{"t1", "t2"}, table.Options.MySQL.Union)

	assert.True(t, optionTruthy(false, "TRUE", 0))
	assert.True(t, optionTruthy(false, "1", 0))
}

func TestTiDBTableOptionHelpers(t *testing.T) {
	p := NewParser()
	table := &core.Table{}

	p.applyTiDBTableOption(&ast.TableOption{Tp: ast.TableOptionAutoIdCache, UintValue: 10}, table)
	assert.Equal(t, uint64(10), table.Options.TiDB.AutoIDCache)

	p.applyTiDBTableOption(&ast.TableOption{Tp: ast.TableOptionAutoRandomBase, UintValue: 5}, table)
	assert.Equal(t, uint64(5), table.Options.TiDB.AutoRandomBase)

	p.applyTiDBTableOption(&ast.TableOption{Tp: ast.TableOptionShardRowID, UintValue: 2}, table)
	assert.Equal(t, uint64(2), table.Options.TiDB.ShardRowID)

	p.applyTiDBTableOption(&ast.TableOption{Tp: ast.TableOptionPreSplitRegion, UintValue: 3}, table)
	assert.Equal(t, uint64(3), table.Options.TiDB.PreSplitRegion)

	p.applyTiDBTableOption(&ast.TableOption{Tp: ast.TableOptionTTLEnable, BoolValue: true}, table)
	assert.True(t, table.Options.TiDB.TTLEnable)

	p.applyTiDBTableOption(&ast.TableOption{Tp: ast.TableOptionTTLJobInterval, StrValue: "1h"}, table)
	assert.Equal(t, "1h", table.Options.TiDB.TTLJobInterval)

	p.applyTiDBTableOption(&ast.TableOption{Tp: ast.TableOptionSequence, StrValue: "ON"}, table)
	assert.True(t, table.Options.TiDB.Sequence)

	p.applyTiDBTableOption(&ast.TableOption{Tp: ast.TableOptionAffinity, StrValue: "aff"}, table)
	assert.Equal(t, "aff", table.Options.TiDB.Affinity)

	p.applyTiDBTableOption(&ast.TableOption{Tp: ast.TableOptionPlacementPolicy, StrValue: "pp"}, table)
	assert.Equal(t, "pp", table.Options.TiDB.PlacementPolicy)

	p.applyTiDBTableOption(&ast.TableOption{Tp: ast.TableOptionStatsBuckets, UintValue: 10}, table)
	assert.Equal(t, uint64(10), table.Options.TiDB.StatsBuckets)

	p.applyTiDBTableOption(&ast.TableOption{Tp: ast.TableOptionStatsTopN, UintValue: 20}, table)
	assert.Equal(t, uint64(20), table.Options.TiDB.StatsTopN)

	p.applyTiDBTableOption(&ast.TableOption{Tp: ast.TableOptionStatsColsChoice, StrValue: "all"}, table)
	assert.Equal(t, "all", table.Options.TiDB.StatsColsChoice)

	p.applyTiDBTableOption(&ast.TableOption{Tp: ast.TableOptionStatsColList, StrValue: "id"}, table)
	assert.Equal(t, "id", table.Options.TiDB.StatsColList)

	assert.False(t, p.applyTiDBTableOption(&ast.TableOption{Tp: ast.TableOptionEngine}, table))

	p.applyTTLOption(&ast.TableOption{
		ColumnName:    &ast.ColumnName{Name: ast.NewCIStr("created_at")},
		TimeUnitValue: &ast.TimeUnitExpr{Unit: ast.TimeUnitDay},
		Value:         ast.NewValueExpr("7", "", ""),
	}, table)
	assert.Contains(t, table.Options.TiDB.TTL, "created_at")
	assert.Contains(t, table.Options.TiDB.TTL, "7")
	assert.Contains(t, table.Options.TiDB.TTL, "DAY")
}

func TestParseTiDBStatsSampleRateOptionErrors(t *testing.T) {
	p := NewParser()
	table := &core.Table{}

	p.parseTiDBStatsSampleRateOption(&ast.TableOption{Tp: ast.TableOptionStatsSampleRate}, table)
	assert.Equal(t, 0.0, table.Options.TiDB.StatsSampleRate)

	p.parseTiDBStatsSampleRateOption(&ast.TableOption{Tp: ast.TableOptionStatsSampleRate, Value: ast.NewValueExpr("not-a-float", "", "")}, table)
	assert.Equal(t, 0.0, table.Options.TiDB.StatsSampleRate)
}
