package toml

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseEmptyTable(t *testing.T) {
	const schema = `
[database]
name = "testdb"
dialect = "mysql"

[[tables]]
name = "empty"
`
	p := NewParser()
	_, err := p.Parse(strings.NewReader(schema))
	assert.Error(t, err)
}

func TestParseDuplicateTableName(t *testing.T) {
	const schema = `
[database]
name = "testdb"
dialect = "mysql"

[[tables]]
name = "items"

  [[tables.columns]]
  name = "id"
  type = "int"
  primary_key = true

[[tables]]
name = "items"

  [[tables.columns]]
  name = "id"
  type = "int"
  primary_key = true
`
	p := NewParser()
	_, err := p.Parse(strings.NewReader(schema))
	assert.Error(t, err)
}

func TestParseTableOptions(t *testing.T) {
	const schema = `
[database]
name = "testdb"
dialect = "mysql"

[[tables]]
name = "items"

  [tables.options]
  tablespace     = "ts1"

  [tables.options.mysql]
  engine         = "InnoDB"
  charset        = "utf8mb4"
  collate        = "utf8mb4_general_ci"
  row_format     = "COMPRESSED"
  compression    = "zlib"
  encryption     = "Y"
  key_block_size = 8

  [[tables.columns]]
  name = "id"
  type = "int"
  primary_key = true
`
	p := NewParser()
	db, err := p.Parse(strings.NewReader(schema))
	require.NoError(t, err)

	opts := db.Tables[0].Options
	assert.Equal(t, "ts1", opts.Tablespace)
	require.NotNil(t, opts.MySQL)
	assert.Equal(t, "InnoDB", opts.MySQL.Engine)
	assert.Equal(t, "utf8mb4", opts.MySQL.Charset)
	assert.Equal(t, "utf8mb4_general_ci", opts.MySQL.Collate)
	assert.Equal(t, "COMPRESSED", opts.MySQL.RowFormat)
	assert.Equal(t, "zlib", opts.MySQL.Compression)
	assert.Equal(t, "Y", opts.MySQL.Encryption)
	assert.Equal(t, uint64(8), opts.MySQL.KeyBlockSize)
}

func TestParseTableOptionsPostgreSQL(t *testing.T) {
	const schema = `
[database]
name = "testdb"
dialect = "mysql"

[[tables]]
name = "items"

  [tables.options]
  tablespace = "pg_default"

  [tables.options.postgresql]
  schema       = "public"
  unlogged     = true
  fillfactor   = 70
  partition_by = "RANGE (created_at)"
  inherits     = ["parent_table", "another_parent"]

  [[tables.columns]]
  name = "id"
  type = "int"
  primary_key = true
`
	p := NewParser()
	db, err := p.Parse(strings.NewReader(schema))
	require.NoError(t, err)

	opts := db.Tables[0].Options
	assert.Equal(t, "pg_default", opts.Tablespace)
	require.NotNil(t, opts.PostgreSQL)
	assert.Equal(t, "public", opts.PostgreSQL.Schema)
	assert.True(t, opts.PostgreSQL.Unlogged)
	assert.Equal(t, 70, opts.PostgreSQL.Fillfactor)
	assert.Equal(t, "RANGE (created_at)", opts.PostgreSQL.PartitionBy)
	assert.Equal(t, []string{"parent_table", "another_parent"}, opts.PostgreSQL.Inherits)
}

func TestParseTableOptionsOracle(t *testing.T) {
	const schema = `
[database]
name = "testdb"
dialect = "mysql"

[[tables]]
name = "items"

  [tables.options]
  tablespace = "users_ts"

  [tables.options.oracle]
  organization     = "INDEX"
  logging          = true
  pctfree          = 20
  pctused          = 40
  init_trans       = 4
  segment_creation = "IMMEDIATE"

  [[tables.columns]]
  name = "id"
  type = "int"
  primary_key = true
`
	p := NewParser()
	db, err := p.Parse(strings.NewReader(schema))
	require.NoError(t, err)

	opts := db.Tables[0].Options
	assert.Equal(t, "users_ts", opts.Tablespace)
	require.NotNil(t, opts.Oracle)
	assert.Equal(t, "INDEX", opts.Oracle.Organization)
	require.NotNil(t, opts.Oracle.Logging)
	assert.True(t, *opts.Oracle.Logging)
	assert.Equal(t, 20, opts.Oracle.Pctfree)
	assert.Equal(t, 40, opts.Oracle.Pctused)
	assert.Equal(t, 4, opts.Oracle.InitTrans)
	assert.Equal(t, "IMMEDIATE", opts.Oracle.SegmentCreation)
}

func TestParseTableOptionsSQLServer(t *testing.T) {
	const schema = `
[database]
name = "testdb"
dialect = "mysql"

[[tables]]
name = "items"

  [tables.options.sqlserver]
  file_group        = "PRIMARY"
  data_compression  = "PAGE"
  memory_optimized  = true
  system_versioning = true
  textimage_on      = "LOB_FG"
  ledger_table      = true

  [[tables.columns]]
  name = "id"
  type = "int"
  primary_key = true
`
	p := NewParser()
	db, err := p.Parse(strings.NewReader(schema))
	require.NoError(t, err)

	opts := db.Tables[0].Options
	require.NotNil(t, opts.SQLServer)
	assert.Equal(t, "PRIMARY", opts.SQLServer.FileGroup)
	assert.Equal(t, "PAGE", opts.SQLServer.DataCompression)
	assert.True(t, opts.SQLServer.MemoryOptimized)
	assert.True(t, opts.SQLServer.SystemVersioning)
	assert.Equal(t, "LOB_FG", opts.SQLServer.TextImageOn)
	assert.True(t, opts.SQLServer.LedgerTable)
}

func TestParseTableOptionsDB2(t *testing.T) {
	const schema = `
[database]
name = "testdb"
dialect = "mysql"

[[tables]]
name = "items"

  [tables.options.db2]
  organize_by  = "COLUMN"
  compress     = "YES"
  data_capture = "CHANGES"
  append_mode  = true
  volatile     = true

  [[tables.columns]]
  name = "id"
  type = "int"
  primary_key = true
`
	p := NewParser()
	db, err := p.Parse(strings.NewReader(schema))
	require.NoError(t, err)

	opts := db.Tables[0].Options
	require.NotNil(t, opts.DB2)
	assert.Equal(t, "COLUMN", opts.DB2.OrganizeBy)
	assert.Equal(t, "YES", opts.DB2.Compress)
	assert.Equal(t, "CHANGES", opts.DB2.DataCapture)
	assert.True(t, opts.DB2.AppendMode)
	assert.True(t, opts.DB2.Volatile)
}

func TestParseTableOptionsSnowflake(t *testing.T) {
	const schema = `
[database]
name = "testdb"
dialect = "mysql"

[[tables]]
name = "items"

  [tables.options.snowflake]
  cluster_by          = ["region", "created_at"]
  data_retention_days = 30
  change_tracking     = true
  copy_grants         = true
  transient           = true

  [[tables.columns]]
  name = "id"
  type = "int"
  primary_key = true
`
	p := NewParser()
	db, err := p.Parse(strings.NewReader(schema))
	require.NoError(t, err)

	opts := db.Tables[0].Options
	require.NotNil(t, opts.Snowflake)
	assert.Equal(t, []string{"region", "created_at"}, opts.Snowflake.ClusterBy)
	require.NotNil(t, opts.Snowflake.DataRetentionDays)
	assert.Equal(t, 30, *opts.Snowflake.DataRetentionDays)
	assert.True(t, opts.Snowflake.ChangeTracking)
	assert.True(t, opts.Snowflake.CopyGrants)
	assert.True(t, opts.Snowflake.Transient)
}

func TestParseTableOptionsSnowflakeZeroRetention(t *testing.T) {
	const schema = `
[database]
name = "testdb"
dialect = "mysql"

[[tables]]
name = "items"

  [tables.options.snowflake]
  data_retention_days = 0

  [[tables.columns]]
  name = "id"
  type = "int"
  primary_key = true
`
	p := NewParser()
	db, err := p.Parse(strings.NewReader(schema))
	require.NoError(t, err)

	opts := db.Tables[0].Options
	require.NotNil(t, opts.Snowflake)
	require.NotNil(t, opts.Snowflake.DataRetentionDays)
	assert.Equal(t, 0, *opts.Snowflake.DataRetentionDays)
}

func TestParseTableOptionsSQLite(t *testing.T) {
	const schema = `
[database]
name = "testdb"
dialect = "mysql"

[[tables]]
name = "items"

  [tables.options.sqlite]
  without_rowid = true
  strict        = true

  [[tables.columns]]
  name = "id"
  type = "int"
  primary_key = true
`
	p := NewParser()
	db, err := p.Parse(strings.NewReader(schema))
	require.NoError(t, err)

	opts := db.Tables[0].Options
	require.NotNil(t, opts.SQLite)
	assert.True(t, opts.SQLite.WithoutRowid)
	assert.True(t, opts.SQLite.Strict)
}

func TestParseTableOptionsTiDB(t *testing.T) {
	const schema = `
[database]
name = "testdb"
dialect = "mysql"

[[tables]]
name = "items"

  [tables.options.tidb]
  auto_id_cache      = 1000
  auto_random_base   = 64
  shard_row_id       = 16
  pre_split_region   = 4
  ttl                = "created_at + INTERVAL 90 DAY"
  ttl_enable         = true
  ttl_job_interval   = "1h"
  affinity           = "zone-a"
  placement_policy   = "global_policy"
  stats_buckets      = 256
  stats_top_n        = 100
  stats_cols_choice  = "LIST"
  stats_col_list     = "id,name,status"
  stats_sample_rate  = 0.5
  sequence           = true

  [[tables.columns]]
  name = "id"
  type = "int"
  primary_key = true
`
	p := NewParser()
	db, err := p.Parse(strings.NewReader(schema))
	require.NoError(t, err)

	opts := db.Tables[0].Options
	require.NotNil(t, opts.TiDB)
	assert.Equal(t, uint64(1000), opts.TiDB.AutoIDCache)
	assert.Equal(t, uint64(64), opts.TiDB.AutoRandomBase)
	assert.Equal(t, uint64(16), opts.TiDB.ShardRowID)
	assert.Equal(t, uint64(4), opts.TiDB.PreSplitRegion)
	assert.Equal(t, "created_at + INTERVAL 90 DAY", opts.TiDB.TTL)
	assert.True(t, opts.TiDB.TTLEnable)
	assert.Equal(t, "1h", opts.TiDB.TTLJobInterval)
	assert.Equal(t, "zone-a", opts.TiDB.Affinity)
	assert.Equal(t, "global_policy", opts.TiDB.PlacementPolicy)
	assert.Equal(t, uint64(256), opts.TiDB.StatsBuckets)
	assert.Equal(t, uint64(100), opts.TiDB.StatsTopN)
	assert.Equal(t, "LIST", opts.TiDB.StatsColsChoice)
	assert.Equal(t, "id,name,status", opts.TiDB.StatsColList)
	assert.InDelta(t, 0.5, opts.TiDB.StatsSampleRate, 0.001)
	assert.True(t, opts.TiDB.Sequence)
}

func TestParseTableOptionsMariaDB(t *testing.T) {
	const schema = `
[database]
name = "testdb"
dialect = "mysql"

[[tables]]
name = "items"

  [tables.options.mariadb]
  page_checksum          = 1
  transactional          = 1
  encryption_key_id      = 5
  sequence               = true
  with_system_versioning = true

  [[tables.columns]]
  name = "id"
  type = "int"
  primary_key = true
`
	p := NewParser()
	db, err := p.Parse(strings.NewReader(schema))
	require.NoError(t, err)

	opts := db.Tables[0].Options
	require.NotNil(t, opts.MariaDB)
	assert.Equal(t, uint64(1), opts.MariaDB.PageChecksum)
	assert.Equal(t, uint64(1), opts.MariaDB.Transactional)
	require.NotNil(t, opts.MariaDB.EncryptionKeyID)
	assert.Equal(t, 5, *opts.MariaDB.EncryptionKeyID)
	assert.True(t, opts.MariaDB.Sequence)
	assert.True(t, opts.MariaDB.WithSystemVersioning)
}

func TestParseTableOptionsMariaDBNilEncryptionKeyID(t *testing.T) {
	const schema = `
[database]
name = "testdb"
dialect = "mysql"

[[tables]]
name = "items"

  [tables.options.mariadb]
  sequence = true

  [[tables.columns]]
  name = "id"
  type = "int"
  primary_key = true
`
	p := NewParser()
	db, err := p.Parse(strings.NewReader(schema))
	require.NoError(t, err)

	opts := db.Tables[0].Options
	require.NotNil(t, opts.MariaDB)
	assert.Nil(t, opts.MariaDB.EncryptionKeyID)
	assert.True(t, opts.MariaDB.Sequence)
}

func TestParseTableOptionsOracleLoggingFalse(t *testing.T) {
	const schema = `
[database]
name = "testdb"
dialect = "mysql"

[[tables]]
name = "items"

  [tables.options.oracle]
  logging = false
  pctfree = 10

  [[tables.columns]]
  name = "id"
  type = "int"
  primary_key = true
`
	p := NewParser()
	db, err := p.Parse(strings.NewReader(schema))
	require.NoError(t, err)

	opts := db.Tables[0].Options
	require.NotNil(t, opts.Oracle)
	require.NotNil(t, opts.Oracle.Logging)
	assert.False(t, *opts.Oracle.Logging)
	assert.Equal(t, 10, opts.Oracle.Pctfree)
}

func TestParseTableOptionsMultipleDialects(t *testing.T) {
	const schema = `
[database]
name = "testdb"
dialect = "mysql"

[[tables]]
name = "items"

  [tables.options]
  tablespace = "shared_ts"

  [tables.options.mysql]
  engine  = "InnoDB"
  charset = "utf8mb4"

  [tables.options.postgresql]
  schema     = "app"
  fillfactor = 90

  [tables.options.sqlite]
  strict = true

  [[tables.columns]]
  name = "id"
  type = "int"
  primary_key = true
`
	p := NewParser()
	db, err := p.Parse(strings.NewReader(schema))
	require.NoError(t, err)

	opts := db.Tables[0].Options
	assert.Equal(t, "shared_ts", opts.Tablespace)

	require.NotNil(t, opts.MySQL)
	assert.Equal(t, "InnoDB", opts.MySQL.Engine)
	assert.Equal(t, "utf8mb4", opts.MySQL.Charset)

	require.NotNil(t, opts.PostgreSQL)
	assert.Equal(t, "app", opts.PostgreSQL.Schema)
	assert.Equal(t, 90, opts.PostgreSQL.Fillfactor)

	require.NotNil(t, opts.SQLite)
	assert.True(t, opts.SQLite.Strict)

	// Other dialects should remain nil.
	assert.Nil(t, opts.Oracle)
	assert.Nil(t, opts.SQLServer)
	assert.Nil(t, opts.DB2)
	assert.Nil(t, opts.Snowflake)
	assert.Nil(t, opts.TiDB)
	assert.Nil(t, opts.MariaDB)
}

func TestParseTableOptionsNoDialectSpecific(t *testing.T) {
	const schema = `
[database]
name = "testdb"
dialect = "mysql"

[[tables]]
name = "items"

  [tables.options]
  tablespace = "default_ts"

  [[tables.columns]]
  name = "id"
  type = "int"
  primary_key = true
`
	p := NewParser()
	db, err := p.Parse(strings.NewReader(schema))
	require.NoError(t, err)

	opts := db.Tables[0].Options
	assert.Equal(t, "default_ts", opts.Tablespace)
	assert.Nil(t, opts.MySQL)
	assert.Nil(t, opts.PostgreSQL)
	assert.Nil(t, opts.Oracle)
	assert.Nil(t, opts.SQLServer)
	assert.Nil(t, opts.DB2)
	assert.Nil(t, opts.Snowflake)
	assert.Nil(t, opts.SQLite)
	assert.Nil(t, opts.TiDB)
	assert.Nil(t, opts.MariaDB)
}

func TestParseTimestampsInjection(t *testing.T) {
	const schema = `
[database]
name = "testdb"
dialect = "mysql"

[[tables]]
name = "items"

  [tables.timestamps]
  enabled = true

  [[tables.columns]]
  name = "id"
  type = "int"
  primary_key = true
`
	p := NewParser()
	db, err := p.Parse(strings.NewReader(schema))
	require.NoError(t, err)

	tbl := db.Tables[0]
	require.NotNil(t, tbl.Timestamps)
	assert.True(t, tbl.Timestamps.Enabled)

	// 1 declared + 2 injected = 3.
	assert.Len(t, tbl.Columns, 3)

	createdAt := tbl.FindColumn("created_at")
	require.NotNil(t, createdAt)
	assert.Equal(t, "timestamp", createdAt.RawType)
	require.NotNil(t, createdAt.DefaultValue)
	assert.Equal(t, "CURRENT_TIMESTAMP", *createdAt.DefaultValue)

	updatedAt := tbl.FindColumn("updated_at")
	require.NotNil(t, updatedAt)
	assert.Equal(t, "timestamp", updatedAt.RawType)
	require.NotNil(t, updatedAt.DefaultValue)
	assert.Equal(t, "CURRENT_TIMESTAMP", *updatedAt.DefaultValue)
	require.NotNil(t, updatedAt.OnUpdate)
	assert.Equal(t, "CURRENT_TIMESTAMP", *updatedAt.OnUpdate)
}

func TestParseTimestampsCustomColumnNames(t *testing.T) {
	const schema = `
[database]
name = "testdb"
dialect = "mysql"

[[tables]]
name = "items"

  [tables.timestamps]
  enabled        = true
  created_column = "inserted_at"
  updated_column = "modified_at"

  [[tables.columns]]
  name = "id"
  type = "int"
  primary_key = true
`
	p := NewParser()
	db, err := p.Parse(strings.NewReader(schema))
	require.NoError(t, err)

	tbl := db.Tables[0]
	assert.Len(t, tbl.Columns, 3)

	assert.NotNil(t, tbl.FindColumn("inserted_at"))
	assert.NotNil(t, tbl.FindColumn("modified_at"))
	assert.Nil(t, tbl.FindColumn("created_at"))
	assert.Nil(t, tbl.FindColumn("updated_at"))
}

func TestParseTimestampsSkipIfColumnsExist(t *testing.T) {
	const schema = `
[database]
name = "testdb"
dialect = "mysql"

[[tables]]
name = "items"

  [tables.timestamps]
  enabled = true

  [[tables.columns]]
  name = "id"
  type = "int"
  primary_key = true

  [[tables.columns]]
  name    = "created_at"
  type    = "timestamp"
  default = "CUSTOM_VALUE"
`
	p := NewParser()
	db, err := p.Parse(strings.NewReader(schema))
	require.NoError(t, err)

	tbl := db.Tables[0]
	// created_at already exists -> not injected again.
	// updated_at doesn't exist -> injected.
	assert.Len(t, tbl.Columns, 3)

	createdAt := tbl.FindColumn("created_at")
	require.NotNil(t, createdAt)
	require.NotNil(t, createdAt.DefaultValue)
	assert.Equal(t, "CUSTOM_VALUE", *createdAt.DefaultValue, "existing column should not be overwritten")
}

func TestParseTimestampsDisabled(t *testing.T) {
	const schema = `
[database]
name = "testdb"
dialect = "mysql"

[[tables]]
name = "items"

  [tables.timestamps]
  enabled = false

  [[tables.columns]]
  name = "id"
  type = "int"
  primary_key = true
`
	p := NewParser()
	db, err := p.Parse(strings.NewReader(schema))
	require.NoError(t, err)

	tbl := db.Tables[0]
	assert.Len(t, tbl.Columns, 1, "timestamps disabled -> no injection")
}

func TestParseTimestampsSameColumnName(t *testing.T) {
	const schema = `
[database]
name = "testdb"
dialect = "mysql"

[[tables]]
name = "items"

  [[tables.columns]]
  name = "id"
  type = "int"
  primary_key = true

  [tables.timestamps]
  enabled        = true
  created_column = "ts"
  updated_column = "ts"
`
	p := NewParser()
	_, err := p.Parse(strings.NewReader(schema))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "same name")
	assert.Contains(t, err.Error(), "ts")
}

func TestParseTimestampsDefaultsSameColumnName(t *testing.T) {
	// Both default to the same name if one overrides to match the other's default.
	const schema = `
[database]
name = "testdb"
dialect = "mysql"

[[tables]]
name = "items"

  [[tables.columns]]
  name = "id"
  type = "int"
  primary_key = true

  [tables.timestamps]
  enabled        = true
  created_column = "updated_at"
`
	p := NewParser()
	_, err := p.Parse(strings.NewReader(schema))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "same name")
}

func TestParseTimestampsDistinctColumnsValid(t *testing.T) {
	const schema = `
[database]
name = "testdb"
dialect = "mysql"

[[tables]]
name = "items"

  [[tables.columns]]
  name = "id"
  type = "int"
  primary_key = true

  [tables.timestamps]
  enabled        = true
  created_column = "inserted_at"
  updated_column = "modified_at"
`
	p := NewParser()
	db, err := p.Parse(strings.NewReader(schema))
	require.NoError(t, err)

	tbl := db.Tables[0]
	assert.NotNil(t, tbl.FindColumn("inserted_at"))
	assert.NotNil(t, tbl.FindColumn("modified_at"))
}

func TestParseTableWithoutPK(t *testing.T) {
	const schema = `
[database]
name = "testdb"
dialect = "mysql"

[[tables]]
name = "logs"

  [[tables.columns]]
  name = "message"
  type = "text"
`
	p := NewParser()
	db, err := p.Parse(strings.NewReader(schema))
	require.NoError(t, err)

	tbl := db.Tables[0]
	assert.Nil(t, tbl.PrimaryKey())
}

func TestParseDistinctColumnsValid(t *testing.T) {
	const schema = `
[database]
name = "testdb"
dialect = "mysql"

[[tables]]
name = "items"

  [[tables.columns]]
  name = "id"
  type = "int"
  primary_key = true

  [[tables.columns]]
  name = "name"
  type = "varchar(255)"

  [[tables.columns]]
  name = "code"
  type = "varchar(50)"
`
	p := NewParser()
	db, err := p.Parse(strings.NewReader(schema))
	require.NoError(t, err)
	assert.Len(t, db.Tables[0].Columns, 3)
}
