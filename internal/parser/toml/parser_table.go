package toml

import (
	"fmt"

	"smf/internal/core"
)

// tomlTable maps [[tables]].
type tomlTable struct {
	Name        string           `toml:"name"`
	Comment     string           `toml:"comment"`
	Options     tomlTableOptions `toml:"options"`
	Columns     []tomlColumn     `toml:"columns"`
	Constraints []tomlConstraint `toml:"constraints"`
	Indexes     []tomlIndex      `toml:"indexes"`
	Timestamps  *tomlTimestamps  `toml:"timestamps"`
}

// tomlTimestamps maps [tables.timestamps].
type tomlTimestamps struct {
	Enabled       bool   `toml:"enabled"`
	CreatedColumn string `toml:"created_column"`
	UpdatedColumn string `toml:"updated_column"`
}

// tomlTableOptions maps [tables.options].
type tomlTableOptions struct {
	Tablespace string `toml:"tablespace"`

	MySQL      *tomlMySQLTableOptions      `toml:"mysql"`
	TiDB       *tomlTiDBTableOptions       `toml:"tidb"`
	PostgreSQL *tomlPostgreSQLTableOptions `toml:"postgresql"`
	Oracle     *tomlOracleTableOptions     `toml:"oracle"`
	SQLServer  *tomlSQLServerTableOptions  `toml:"sqlserver"`
	DB2        *tomlDB2TableOptions        `toml:"db2"`
	Snowflake  *tomlSnowflakeTableOptions  `toml:"snowflake"`
	SQLite     *tomlSQLiteTableOptions     `toml:"sqlite"`
	MariaDB    *tomlMariaDBTableOptions    `toml:"mariadb"`
}

// tomlMySQLTableOptions maps [tables.options.mysql].
type tomlMySQLTableOptions struct {
	Engine                   string   `toml:"engine"`
	Charset                  string   `toml:"charset"`
	Collate                  string   `toml:"collate"`
	AutoIncrement            uint64   `toml:"auto_increment"`
	RowFormat                string   `toml:"row_format"`
	AvgRowLength             uint64   `toml:"avg_row_length"`
	KeyBlockSize             uint64   `toml:"key_block_size"`
	MaxRows                  uint64   `toml:"max_rows"`
	MinRows                  uint64   `toml:"min_rows"`
	Checksum                 uint64   `toml:"checksum"`
	DelayKeyWrite            uint64   `toml:"delay_key_write"`
	Compression              string   `toml:"compression"`
	Encryption               string   `toml:"encryption"`
	PackKeys                 string   `toml:"pack_keys"`
	DataDirectory            string   `toml:"data_directory"`
	IndexDirectory           string   `toml:"index_directory"`
	InsertMethod             string   `toml:"insert_method"`
	StorageMedia             string   `toml:"storage_media"`
	StatsPersistent          string   `toml:"stats_persistent"`
	StatsAutoRecalc          string   `toml:"stats_auto_recalc"`
	StatsSamplePages         string   `toml:"stats_sample_pages"`
	Connection               string   `toml:"connection"`
	Password                 string   `toml:"password"`
	AutoextendSize           string   `toml:"autoextend_size"`
	Union                    []string `toml:"union"`
	SecondaryEngine          string   `toml:"secondary_engine"`
	TableChecksum            uint64   `toml:"table_checksum"`
	EngineAttribute          string   `toml:"engine_attribute"`
	SecondaryEngineAttribute string   `toml:"secondary_engine_attribute"`
	PageCompressed           bool     `toml:"page_compressed"`
	PageCompressionLevel     uint64   `toml:"page_compression_level"`
	IetfQuotes               bool     `toml:"ietf_quotes"`
	Nodegroup                uint64   `toml:"nodegroup"`
}

// tomlTiDBTableOptions maps [tables.options.tidb].
type tomlTiDBTableOptions struct {
	AutoIDCache     uint64  `toml:"auto_id_cache"`
	AutoRandomBase  uint64  `toml:"auto_random_base"`
	ShardRowID      uint64  `toml:"shard_row_id"`
	PreSplitRegion  uint64  `toml:"pre_split_region"`
	TTL             string  `toml:"ttl"`
	TTLEnable       bool    `toml:"ttl_enable"`
	TTLJobInterval  string  `toml:"ttl_job_interval"`
	Affinity        string  `toml:"affinity"`
	PlacementPolicy string  `toml:"placement_policy"`
	StatsBuckets    uint64  `toml:"stats_buckets"`
	StatsTopN       uint64  `toml:"stats_top_n"`
	StatsColsChoice string  `toml:"stats_cols_choice"`
	StatsColList    string  `toml:"stats_col_list"`
	StatsSampleRate float64 `toml:"stats_sample_rate"`
	Sequence        bool    `toml:"sequence"`
}

// tomlPostgreSQLTableOptions maps [tables.options.postgresql].
type tomlPostgreSQLTableOptions struct {
	Schema      string   `toml:"schema"`
	Unlogged    bool     `toml:"unlogged"`
	Fillfactor  int      `toml:"fillfactor"`
	PartitionBy string   `toml:"partition_by"`
	Inherits    []string `toml:"inherits"`
}

// tomlOracleTableOptions maps [tables.options.oracle].
type tomlOracleTableOptions struct {
	Organization    string `toml:"organization"`
	Logging         *bool  `toml:"logging"`
	Pctfree         int    `toml:"pctfree"`
	Pctused         int    `toml:"pctused"`
	InitTrans       int    `toml:"init_trans"`
	SegmentCreation string `toml:"segment_creation"`
}

// tomlSQLServerTableOptions maps [tables.options.sqlserver].
type tomlSQLServerTableOptions struct {
	FileGroup        string `toml:"file_group"`
	DataCompression  string `toml:"data_compression"`
	MemoryOptimized  bool   `toml:"memory_optimized"`
	SystemVersioning bool   `toml:"system_versioning"`
	TextImageOn      string `toml:"textimage_on"`
	LedgerTable      bool   `toml:"ledger_table"`
}

// tomlDB2TableOptions maps [tables.options.db2].
type tomlDB2TableOptions struct {
	OrganizeBy  string `toml:"organize_by"`
	Compress    string `toml:"compress"`
	DataCapture string `toml:"data_capture"`
	AppendMode  bool   `toml:"append_mode"`
	Volatile    bool   `toml:"volatile"`
}

// tomlSnowflakeTableOptions maps [tables.options.snowflake].
type tomlSnowflakeTableOptions struct {
	ClusterBy         []string `toml:"cluster_by"`
	DataRetentionDays *int     `toml:"data_retention_days"`
	ChangeTracking    bool     `toml:"change_tracking"`
	CopyGrants        bool     `toml:"copy_grants"`
	Transient         bool     `toml:"transient"`
}

// tomlSQLiteTableOptions maps [tables.options.sqlite].
type tomlSQLiteTableOptions struct {
	WithoutRowid bool `toml:"without_rowid"`
	Strict       bool `toml:"strict"`
}

// tomlMariaDBTableOptions maps [tables.options.mariadb].
type tomlMariaDBTableOptions struct {
	PageChecksum         uint64 `toml:"page_checksum"`
	Transactional        uint64 `toml:"transactional"`
	EncryptionKeyID      *int   `toml:"encryption_key_id"`
	Sequence             bool   `toml:"sequence"`
	WithSystemVersioning bool   `toml:"with_system_versioning"`
}

func (p *Parser) parseTable(tt *tomlTable) (*core.Table, error) {
	table := &core.Table{
		Name:    tt.Name,
		Comment: tt.Comment,
		Options: parseTableOptions(&tt.Options),
	}

	if ts := tt.Timestamps; ts != nil {
		table.Timestamps = &core.TimestampsConfig{
			Enabled:       ts.Enabled,
			CreatedColumn: ts.CreatedColumn,
			UpdatedColumn: ts.UpdatedColumn,
		}
	}

	if err := p.parseTableColumns(table, tt); err != nil {
		return nil, err
	}

	table.Constraints = make([]*core.Constraint, 0, len(tt.Constraints))
	for i := range tt.Constraints {
		con := parseTableConstraint(&tt.Constraints[i])
		table.Constraints = append(table.Constraints, con)
	}

	table.Indexes = make([]*core.Index, 0, len(tt.Indexes))
	for i := range tt.Indexes {
		idx := parseTableIndex(&tt.Indexes[i])
		table.Indexes = append(table.Indexes, idx)
	}

	return table, nil
}

func parseTableOptions(to *tomlTableOptions) core.TableOptions {
	opts := core.TableOptions{
		Tablespace: to.Tablespace,
	}

	if to.MySQL != nil {
		opts.MySQL = parseMySQLTableOptions(to.MySQL)
	}
	if to.TiDB != nil {
		opts.TiDB = parseTiDBTableOptions(to.TiDB)
	}
	if to.PostgreSQL != nil {
		opts.PostgreSQL = parsePostgreSQLTableOptions(to.PostgreSQL)
	}
	if to.Oracle != nil {
		opts.Oracle = parseOracleTableOptions(to.Oracle)
	}
	if to.SQLServer != nil {
		opts.SQLServer = parseSQLServerTableOptions(to.SQLServer)
	}
	if to.DB2 != nil {
		opts.DB2 = parseDB2TableOptions(to.DB2)
	}
	if to.Snowflake != nil {
		opts.Snowflake = parseSnowflakeTableOptions(to.Snowflake)
	}
	if to.SQLite != nil {
		opts.SQLite = parseSQLiteTableOptions(to.SQLite)
	}
	if to.MariaDB != nil {
		opts.MariaDB = parseMariaDBTableOptions(to.MariaDB)
	}

	return opts
}

func parseMySQLTableOptions(m *tomlMySQLTableOptions) *core.MySQLTableOptions {
	return &core.MySQLTableOptions{
		Engine:                   m.Engine,
		Charset:                  m.Charset,
		Collate:                  m.Collate,
		AutoIncrement:            m.AutoIncrement,
		RowFormat:                m.RowFormat,
		AvgRowLength:             m.AvgRowLength,
		KeyBlockSize:             m.KeyBlockSize,
		MaxRows:                  m.MaxRows,
		MinRows:                  m.MinRows,
		Checksum:                 m.Checksum,
		DelayKeyWrite:            m.DelayKeyWrite,
		Compression:              m.Compression,
		Encryption:               m.Encryption,
		PackKeys:                 m.PackKeys,
		DataDirectory:            m.DataDirectory,
		IndexDirectory:           m.IndexDirectory,
		InsertMethod:             m.InsertMethod,
		StorageMedia:             m.StorageMedia,
		StatsPersistent:          m.StatsPersistent,
		StatsAutoRecalc:          m.StatsAutoRecalc,
		StatsSamplePages:         m.StatsSamplePages,
		Connection:               m.Connection,
		Password:                 m.Password,
		AutoextendSize:           m.AutoextendSize,
		Union:                    m.Union,
		SecondaryEngine:          m.SecondaryEngine,
		TableChecksum:            m.TableChecksum,
		EngineAttribute:          m.EngineAttribute,
		SecondaryEngineAttribute: m.SecondaryEngineAttribute,
		PageCompressed:           m.PageCompressed,
		PageCompressionLevel:     m.PageCompressionLevel,
		IetfQuotes:               m.IetfQuotes,
		Nodegroup:                m.Nodegroup,
	}
}

func parseTiDBTableOptions(t *tomlTiDBTableOptions) *core.TiDBTableOptions {
	return &core.TiDBTableOptions{
		AutoIDCache:     t.AutoIDCache,
		AutoRandomBase:  t.AutoRandomBase,
		ShardRowID:      t.ShardRowID,
		PreSplitRegion:  t.PreSplitRegion,
		TTL:             t.TTL,
		TTLEnable:       t.TTLEnable,
		TTLJobInterval:  t.TTLJobInterval,
		Affinity:        t.Affinity,
		PlacementPolicy: t.PlacementPolicy,
		StatsBuckets:    t.StatsBuckets,
		StatsTopN:       t.StatsTopN,
		StatsColsChoice: t.StatsColsChoice,
		StatsColList:    t.StatsColList,
		StatsSampleRate: t.StatsSampleRate,
		Sequence:        t.Sequence,
	}
}

func parsePostgreSQLTableOptions(pg *tomlPostgreSQLTableOptions) *core.PostgreSQLTableOptions {
	return &core.PostgreSQLTableOptions{
		Schema:      pg.Schema,
		Unlogged:    pg.Unlogged,
		Fillfactor:  pg.Fillfactor,
		PartitionBy: pg.PartitionBy,
		Inherits:    pg.Inherits,
	}
}

func parseOracleTableOptions(o *tomlOracleTableOptions) *core.OracleTableOptions {
	return &core.OracleTableOptions{
		Organization:    o.Organization,
		Logging:         o.Logging,
		Pctfree:         o.Pctfree,
		Pctused:         o.Pctused,
		InitTrans:       o.InitTrans,
		SegmentCreation: o.SegmentCreation,
	}
}

func parseSQLServerTableOptions(ss *tomlSQLServerTableOptions) *core.SQLServerTableOptions {
	return &core.SQLServerTableOptions{
		FileGroup:        ss.FileGroup,
		DataCompression:  ss.DataCompression,
		MemoryOptimized:  ss.MemoryOptimized,
		SystemVersioning: ss.SystemVersioning,
		TextImageOn:      ss.TextImageOn,
		LedgerTable:      ss.LedgerTable,
	}
}

func parseDB2TableOptions(d *tomlDB2TableOptions) *core.DB2TableOptions {
	return &core.DB2TableOptions{
		OrganizeBy:  d.OrganizeBy,
		Compress:    d.Compress,
		DataCapture: d.DataCapture,
		AppendMode:  d.AppendMode,
		Volatile:    d.Volatile,
	}
}

func parseSnowflakeTableOptions(sf *tomlSnowflakeTableOptions) *core.SnowflakeTableOptions {
	return &core.SnowflakeTableOptions{
		ClusterBy:         sf.ClusterBy,
		DataRetentionDays: sf.DataRetentionDays,
		ChangeTracking:    sf.ChangeTracking,
		CopyGrants:        sf.CopyGrants,
		Transient:         sf.Transient,
	}
}

func parseSQLiteTableOptions(sl *tomlSQLiteTableOptions) *core.SQLiteTableOptions {
	return &core.SQLiteTableOptions{
		WithoutRowid: sl.WithoutRowid,
		Strict:       sl.Strict,
	}
}

func parseMariaDBTableOptions(mdb *tomlMariaDBTableOptions) *core.MariaDBTableOptions {
	return &core.MariaDBTableOptions{
		PageChecksum:         mdb.PageChecksum,
		Transactional:        mdb.Transactional,
		EncryptionKeyID:      mdb.EncryptionKeyID,
		Sequence:             mdb.Sequence,
		WithSystemVersioning: mdb.WithSystemVersioning,
	}
}

// parseTableColumns populates table.Columns from the TOML column definitions
// and injects timestamp columns when enabled.
func (p *Parser) parseTableColumns(table *core.Table, tt *tomlTable) error {
	table.Columns = make([]*core.Column, 0, len(tt.Columns))
	for i := range tt.Columns {
		col, err := p.parseColumn(&tt.Columns[i])
		if err != nil {
			return fmt.Errorf("column %q: %w", tt.Columns[i].Name, err)
		}
		table.Columns = append(table.Columns, col)
	}

	if table.Timestamps != nil && table.Timestamps.Enabled {
		injectTimestampColumns(table)
	}

	return nil
}

// injectTimestampColumns resolves the created/updated column names and appends
// the columns when not already present.
// Note: Validation of distinct column names is done in core.Validate().
func injectTimestampColumns(table *core.Table) {
	createdCol := "created_at"
	updatedCol := "updated_at"
	if table.Timestamps.CreatedColumn != "" {
		createdCol = table.Timestamps.CreatedColumn
	}
	if table.Timestamps.UpdatedColumn != "" {
		updatedCol = table.Timestamps.UpdatedColumn
	}

	hasCreatedCol := func() bool {
		for _, c := range table.Columns {
			if c.Name == createdCol {
				return true
			}
		}
		return false
	}
	hasUpdatedCol := func() bool {
		for _, c := range table.Columns {
			if c.Name == updatedCol {
				return true
			}
		}
		return false
	}

	if !hasCreatedCol() {
		table.Columns = append(table.Columns, &core.Column{
			Name:         createdCol,
			RawType:      "timestamp",
			Type:         core.DataTypeDatetime,
			DefaultValue: new("CURRENT_TIMESTAMP"),
		})
	}

	if !hasUpdatedCol() {
		table.Columns = append(table.Columns, &core.Column{
			Name:         updatedCol,
			RawType:      "timestamp",
			Type:         core.DataTypeDatetime,
			DefaultValue: new("CURRENT_TIMESTAMP"),
			OnUpdate:     new("CURRENT_TIMESTAMP"),
		})
	}
}
