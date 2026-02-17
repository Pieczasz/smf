// Package core contains the single source of truth from the database schema.
// It provides a structured representation of data for tables, columns, constraints, and so on
// for all databases that we support.
package core

import (
	"fmt"
	"strings"
)

// Database represents a database in the schema.
type Database struct {
	Name       string
	Dialect    *Dialect
	Tables     []*Table
	Validation *ValidationRules
}

// Dialect identifies a supported SQL dialect.
type Dialect string

const (
	DialectMySQL      Dialect = "mysql"
	DialectMariaDB    Dialect = "mariadb"
	DialectPostgreSQL Dialect = "postgresql"
	DialectSQLite     Dialect = "sqlite"
	DialectOracle     Dialect = "oracle"
	DialectDB2        Dialect = "db2"
	DialectSnowflake  Dialect = "snowflake"
	DialectMSSQL      Dialect = "mssql"
	DialectTiDB       Dialect = "tidb"
)

// SupportedDialects returns a slice of all supported dialect values.
func SupportedDialects() []Dialect {
	return []Dialect{
		DialectMySQL,
		DialectMariaDB,
		DialectPostgreSQL,
		DialectSQLite,
		DialectOracle,
		DialectDB2,
		DialectSnowflake,
		DialectMSSQL,
		DialectTiDB,
	}
}

// ValidDialect reports whether d is a recognized dialect string.
func ValidDialect(d string) bool {
	for _, supported := range SupportedDialects() {
		if strings.EqualFold(string(supported), d) {
			return true
		}
	}
	return false
}

// ValidationRules configures schema-level validation constraints.
type ValidationRules struct {
	MaxTableNameLength          int    `json:"maxTableNameLength,omitempty"`
	MaxColumnNameLength         int    `json:"maxColumnNameLength,omitempty"`
	AutoGenerateConstraintNames bool   `json:"autoGenerateConstraintNames,omitempty"`
	AllowedNamePattern          string `json:"allowedNamePattern,omitempty"`
}

// Table represents a table in the schema.
// All table names must be in snake_case.
type Table struct {
	Name        string            `json:"name"`
	Columns     []*Column         `json:"columns"`
	Constraints []*Constraint     `json:"constraints,omitempty"`
	Indexes     []*Index          `json:"indexes,omitempty"`
	Comment     string            `json:"comment,omitempty"`
	Options     TableOptions      `json:"options"`
	Timestamps  *TimestampsConfig `json:"timestamps,omitempty"`
}

// TimestampsConfig controls automatic created_at / updated_at column injection.
type TimestampsConfig struct {
	Enabled       bool   `json:"enabled"`
	CreatedColumn string `json:"createdColumn,omitempty"` // Defaults to "created_at".
	UpdatedColumn string `json:"updatedColumn,omitempty"` // Defaults to "updated_at".
}

// TableOptions holds cross-dialect table options and dialect-specific
// option groups.
//
// Only fields that are meaningful across multiple SQL dialects live here.
// Dialect-specific options belong in their respective sub-structs
// (MySQLTableOptions, MariaDBTableOptions, etc.).
type TableOptions struct {
	// Tablespace assigns the table to a named tablespace.
	// Supported by MySQL/InnoDB, Oracle, DB2, and PostgreSQL.
	Tablespace string

	// Dialect-specific option groups.
	MySQL      *MySQLTableOptions      `json:"MySQL,omitempty"`
	TiDB       *TiDBTableOptions       `json:"TiDB,omitempty"`
	PostgreSQL *PostgreSQLTableOptions `json:"PostgreSQL,omitempty"`
	Oracle     *OracleTableOptions     `json:"Oracle,omitempty"`
	SQLServer  *SQLServerTableOptions  `json:"SQLServer,omitempty"`
	DB2        *DB2TableOptions        `json:"DB2,omitempty"`
	Snowflake  *SnowflakeTableOptions  `json:"Snowflake,omitempty"`
	SQLite     *SQLiteTableOptions     `json:"SQLite,omitempty"`
	MariaDB    *MariaDBTableOptions    `json:"MariaDB,omitempty"`
}

// MySQLTableOptions contains MySQL-family table options.
//
// These options map to CREATE TABLE clauses shared by MySQL and MariaDB
// (Engine, Charset, Collate, RowFormat, …) as well as MySQL-only
// features such as secondary engines (HeatWave), NDB Cluster node
// groups, and FEDERATED table connection strings.  MariaDB generators
// should read these for shared options and additionally consult
// MariaDBTableOptions for MariaDB-specific divergences.
type MySQLTableOptions struct {
	// Engine is the storage engine (e.g. "InnoDB", "MyISAM", "Aria").
	Engine string
	// Charset is the default character set for the table (e.g. "utf8mb4").
	Charset string
	// Collate is the default collation for the table (e.g. "utf8mb4_unicode_ci").
	Collate string
	// AutoIncrement sets the starting AUTO_INCREMENT value for the table.
	AutoIncrement uint64

	// RowFormat controls the physical row storage format (e.g. "DYNAMIC", "COMPRESSED", "COMPACT").
	RowFormat string
	// AvgRowLength is a hint for the average row length in bytes, used by the optimizer.
	AvgRowLength uint64
	// KeyBlockSize sets the page size in KB for compressed InnoDB tables.
	KeyBlockSize uint64
	// MaxRows is a hint for the maximum number of rows the table is expected to hold.
	MaxRows uint64
	// MinRows is a hint for the minimum number of rows the table is expected to hold.
	MinRows uint64
	// Checksum enables live table checksum computation (1 = enabled, 0 = disabled).
	Checksum uint64
	// DelayKeyWrite delays key-buffer flushes for MyISAM tables (1 = enabled).
	DelayKeyWrite uint64
	// Compression sets the page-level compression algorithm ("ZLIB", "LZ4", "NONE").
	Compression string
	// Encryption enables transparent data encryption for the tablespace ("Y" or "N").
	Encryption string
	// PackKeys controls index packing for MyISAM tables ("0", "1", or "DEFAULT").
	PackKeys string
	// DataDirectory specifies the OS directory for the table data file (MyISAM / InnoDB file-per-table).
	DataDirectory string
	// IndexDirectory specifies the OS directory for the MyISAM index file.
	IndexDirectory string
	// InsertMethod controls how rows are inserted into a MERGE table ("NO", "FIRST", "LAST").
	InsertMethod string
	// StorageMedia specifies the storage medium for NDB Cluster ("DISK" or "MEMORY").
	StorageMedia string

	// StatsPersistent controls whether InnoDB table statistics are persisted to disk ("0", "1", or "DEFAULT").
	StatsPersistent string
	// StatsAutoRecalc controls whether InnoDB statistics are recalculated automatically ("0", "1", or "DEFAULT").
	StatsAutoRecalc string
	// StatsSamplePages sets the number of index pages sampled for InnoDB statistics estimates.
	StatsSamplePages string

	// Connection is a connection string for a FEDERATED table linking to a remote server.
	Connection string
	// Password is the password used by a FEDERATED table's connection string.
	Password string

	// AutoextendSize sets the InnoDB tablespace auto-extend chunk size.
	AutoextendSize string

	// Union lists the underlying MyISAM tables that form a MERGE table.
	Union []string
	// SecondaryEngine names the secondary engine for HeatWave / RAPID offload (e.g. "RAPID").
	SecondaryEngine string
	// TableChecksum enables per-row checksum stored in the table (NDB Cluster).
	TableChecksum uint64
	// EngineAttribute is an opaque JSON string passed to the primary storage engine.
	EngineAttribute string
	// SecondaryEngineAttribute is an opaque JSON string passed to the secondary engine.
	SecondaryEngineAttribute string
	// PageCompressed enables InnoDB page-level compression (MySQL 5.7+).
	PageCompressed bool
	// PageCompressionLevel sets the zlib compression level for page compression (1-9).
	PageCompressionLevel uint64
	// IetfQuotes enables IETF-compliant quoting for CSV storage engine output.
	IetfQuotes bool
	// Nodegroup assigns the table to an NDB Cluster node group.
	Nodegroup uint64
}

// TiDBTableOptions contains TiDB-specific table options.
//
// TiDB extends MySQL syntax with distributed-database features such as
// row-ID sharding, region pre-splitting, TTL-based data lifecycle, and
// placement policies for multi-datacenter deployments.
type TiDBTableOptions struct {
	// AutoIDCache sets the auto-ID cache size per TiDB node to reduce ID allocation RPCs.
	AutoIDCache uint64
	// AutoRandomBase sets the starting shard bits base for AUTO_RANDOM columns.
	AutoRandomBase uint64
	// ShardRowID enables implicit row-ID sharding to scatter hotspot writes across TiKV regions.
	ShardRowID uint64
	// PreSplitRegion pre-splits the table into 2^n regions at creation time for writing parallelism.
	PreSplitRegion uint64
	// TTL is the time-to-live expression for automatic row expiration (e.g. "created_at + INTERVAL 90 DAY").
	TTL string
	// TTLEnable activates or suspends TTL-based row deletion for this table.
	TTLEnable bool
	// TTLJobInterval controls how frequently the TTL background job runs (e.g. "1h").
	TTLJobInterval string
	// Affinity sets the follower-read affinity label for tidb_replica_read.
	Affinity string
	// PlacementPolicy assigns a placement policy that controls replica placement across datacenters.
	PlacementPolicy string
	// StatsBuckets sets the number of histogram buckets used for table statistics.
	StatsBuckets uint64
	// StatsTopN sets the number of top-N values tracked in column statistics.
	StatsTopN uint64
	// StatsColsChoice controls which columns collect statistics ("DEFAULT", "ALL", "LIST").
	StatsColsChoice string
	// StatsColList is a comma-separated list of columns to collect statistics for when StatsColsChoice is "LIST".
	StatsColList string
	// StatsSampleRate is the sampling rate (0.0-1.0) used when collecting table statistics.
	StatsSampleRate float64
	// Sequence marks the table as backed by a TiDB SEQUENCE object for custom ID generation.
	Sequence bool
}

// PostgreSQLTableOptions contains PostgreSQL-specific table options.
//
// PostgreSQL uses schemas for namespace isolation, UNLOGGED tables for
// ephemeral data, storage parameters like fillfactor, and native
// partitioning via PARTITION BY.
type PostgreSQLTableOptions struct {
	// Schema is the PostgreSQL schema namespace (e.g. "public").
	Schema string `json:"schema,omitempty"`
	// Unlogged creates an UNLOGGED table (not WAL-logged, lost in a crash).
	Unlogged bool `json:"unlogged,omitempty"`
	// Fillfactor controls the packing density of heap pages (10-100).
	Fillfactor int `json:"fillfactor,omitempty"`
	// PartitionBy holds the PARTITION BY clause (e.g. "RANGE (created_at)").
	PartitionBy string `json:"partition_by,omitempty"`
	// Inherits lists parent tables for table inheritance.
	Inherits []string `json:"inherits,omitempty"`
}

type PostgresColumnOptions struct {
	// Storage is the per-attribute storage mode: "PLAIN", "MAIN",
	// "EXTERNAL", "EXTENDED", or "DEFAULT".
	Storage string `json:"storage,omitempty"`

	// Compression sets the TOAST compression method: "pglz" or "lz4".
	Compression string `json:"compression,omitempty"`
}

// OracleTableOptions contains Oracle-specific table options.
//
// Oracle uses tablespace placement, heap/IOT organization, PCT parameters
// for storage tuning, and segment-level creation control.
type OracleTableOptions struct {
	// Organization is the table organization: "HEAP" (default) or "INDEX" (IOT).
	Organization string `json:"organization,omitempty"`
	// Logging controls redo-log generation (true = LOGGING, false = NOLOGGING).
	Logging *bool `json:"logging,omitempty"`
	// Pctfree is the percentage of each block kept free for updates (0-99).
	Pctfree int `json:"pctfree,omitempty"`
	// Pctused is the minimum used-space percentage before new inserts (1-99).
	Pctused int `json:"pctused,omitempty"`
	// InitTrans is the initial number of concurrent transactions per block.
	InitTrans int `json:"initrans,omitempty"`
	// SegmentCreation controls segment allocation: "IMMEDIATE" or "DEFERRED".
	SegmentCreation string `json:"segment_creation,omitempty"`
}

// SQLServerTableOptions contains Microsoft SQL Server / Azure SQL options.
//
// SQL Server uses filegroups instead of tablespaces, page/row/columnstore
// compression, memory-optimized tables (In-Memory OLTP), and temporal
// tables via system versioning.
type SQLServerTableOptions struct {
	// FileGroup is the filegroup for table storage (like tablespace).
	FileGroup string `json:"file_group,omitempty"`
	// DataCompression specifies compression: "NONE", "ROW", "PAGE", or "COLUMNSTORE".
	DataCompression string `json:"data_compression,omitempty"`
	// MemoryOptimized enables In-Memory OLTP (memory-optimized table).
	MemoryOptimized bool `json:"memory_optimized,omitempty"`
	// SystemVersioning enables temporal table support (system-versioned).
	SystemVersioning bool `json:"system_versioning,omitempty"`
	// TextImageOn specifies the filegroup for TEXT/IMAGE/LOB data.
	TextImageOn string `json:"textimage_on,omitempty"`
	// LedgerTable enables the ledger (append-only) table feature in Azure SQL.
	LedgerTable bool `json:"ledger_table,omitempty"`
}

// DB2TableOptions contains IBM DB2-specific table options.
//
// DB2 supports row vs. column organization, table-level compression,
// data capture for replication, and append mode for insert-heavy workloads.
type DB2TableOptions struct {
	// OrganizeBy controls storage layout: "ROW" (default) or "COLUMN".
	OrganizeBy string `json:"organize_by,omitempty"`
	// Compress enables table compression: "YES", "NO", or "" (default).
	Compress string `json:"compress,omitempty"`
	// DataCapture enables change-data-capture: "NONE" or "CHANGES".
	DataCapture string `json:"data_capture,omitempty"`
	// AppendMode enables append mode (no free-space search on INSERT).
	AppendMode bool `json:"append_mode,omitempty"`
	// Volatile marks the table cardinality as highly volatile for the optimizer.
	Volatile bool `json:"volatile,omitempty"`
}

// SnowflakeTableOptions contains Snowflake-specific table options.
//
// Snowflake has no user-managed indexes.  Instead, it offers automatic
// clustering, Time Travel via retention days, change tracking for
// streams, and transient tables that skip Fail-safe.
type SnowflakeTableOptions struct {
	// ClusterBy lists columns/expressions for automatic clustering.
	ClusterBy []string `json:"cluster_by,omitempty"`
	// DataRetentionDays is the Time Travel retention period in days (0-90).
	DataRetentionDays *int `json:"data_retention_days,omitempty"`
	// ChangeTracking enables change tracking for Snowflake streams.
	ChangeTracking bool `json:"change_tracking,omitempty"`
	// CopyGrants preserves grants when recreating the table with CREATE OR REPLACE.
	CopyGrants bool `json:"copy_grants,omitempty"`
	// Transient creates a transient table (no Fail-safe period).
	Transient bool `json:"transient,omitempty"`
}

// SQLiteTableOptions contains SQLite-specific table options.
//
// SQLite is deliberately minimal.  WITHOUT ROWID tables use a clustered
// primary-key B-tree (no hidden rowid column).  STRICT mode (3.37+)
// enforces column type affinity.
type SQLiteTableOptions struct {
	// WithoutRowid creates a WITHOUT ROWID table (clustered PK, no hidden rowid).
	WithoutRowid bool `json:"without_rowid,omitempty"`
	// Strict enables STRICT mode that enforces column type affinity (SQLite 3.37+).
	Strict bool `json:"strict,omitempty"`
}

// MariaDBTableOptions contains MariaDB-specific table options that differ
// from MySQL.
//
// MariaDB shares most CREATE TABLE options with MySQL (stored in
// MySQLTableOptions).  This struct holds only the MariaDB-specific
// divergences: Aria-engine page checksums, transactional mode,
// encryption key management, sequence objects, and system versioning.
type MariaDBTableOptions struct {
	// PageChecksum enables page-level checksums for Aria storage engine tables.
	PageChecksum uint64 `json:"page_checksum,omitempty"`
	// Transactional enables transactional support for Aria storage engine tables.
	Transactional uint64 `json:"transactional,omitempty"`
	// EncryptionKeyID specifies the encryption key ID for table encryption.
	EncryptionKeyID *int `json:"encryption_key_id,omitempty"`
	// Sequence marks the table as a SEQUENCE object (MariaDB 10.3+).
	Sequence bool `json:"sequence,omitempty"`
	// WithSystemVersioning enables system-versioned (temporal) table.
	WithSystemVersioning bool `json:"with_system_versioning,omitempty"`
}

// IdentityGeneration controls the GENERATED clause for identity columns.
type IdentityGeneration string

const (
	IdentityAlways    IdentityGeneration = "ALWAYS"
	IdentityByDefault IdentityGeneration = "BY DEFAULT"
)

// Column represents a single column inside schema.
type Column struct {
	// Name is the column identifier as declared in the schema.
	Name string `json:"name"`
	// RawType is the SQL type string to use for DDL generation (e.g. "VARCHAR(255)", "JSONB").
	// When empty, the generator maps the portable Type to a dialect-specific default.
	RawType string `json:"rawType"`
	// Type is the normalized portable data type category (e.g., DataTypeString).
	// Always derived from the portable TOML `type` field for consistent classification.
	Type DataType `json:"type"`
	// Nullable indicates whether the column allows NULL values.
	Nullable bool `json:"nullable"`
	// PrimaryKey marks this column as part of the table's primary key.
	PrimaryKey bool `json:"primaryKey"`
	// AutoIncrement enables automatic incrementing for this column (MySQL, MariaDB, SQLite).
	AutoIncrement bool `json:"autoIncrement"`
	// DefaultValue is the column's DEFAULT expression (nil means no default).
	DefaultValue *string `json:"defaultValue,omitempty"`
	// OnUpdate is the ON UPDATE expression, typically "CURRENT_TIMESTAMP" (MySQL/MariaDB).
	OnUpdate *string `json:"onUpdate,omitempty"`
	// Comment is an optional descriptive comment stored with the column metadata.
	Comment string `json:"comment,omitempty"`
	// Collate overrides the column-level collation (e.g. "utf8mb4_bin").
	Collate string `json:"collate,omitempty"`
	// Charset overrides the column-level character set (e.g. "utf8mb4").
	Charset string `json:"charset,omitempty"`

	// Unique marks this column as having a UNIQUE constraint.
	// The parser auto-synthesizes a named UNIQUE constraint from this flag.
	Unique bool `json:"unique,omitempty"`

	// Check holds an inline CHECK expression for this column.
	// The parser auto-synthesizes a named CHECK constraint from this field.
	Check string `json:"check,omitempty"`

	// References are inline foreign-key shorthand in "table.column" format.
	// The parser auto-synthesizes a named FOREIGN KEY constraint from this field.
	References string `json:"references,omitempty"`

	// RefOnDelete is the ON DELETE referential action for an inline FK.
	RefOnDelete ReferentialAction `json:"refOnDelete,omitempty"`

	// RefOnUpdate is the ON UPDATE referential action for an inline FK.
	RefOnUpdate ReferentialAction `json:"refOnUpdate,omitempty"`

	// EnumValues holds the allowed values when Type is "enum".
	// In TOML this is written as values = ["free", "pro", "enterprise"]
	// which is cleaner and safer than embedding quotes in the type string.
	EnumValues []string `json:"enumValues,omitempty"`

	// IdentitySeed is the starting value for IDENTITY / auto-increment columns.
	// Used by MSSQL (IDENTITY(seed,increment)), DB2 (START WITH), and
	// Snowflake (IDENTITY(start, step)).  Zero means "use the dialect default" (usually 1).
	IdentitySeed int64 `json:"identitySeed,omitempty"`

	// IdentityIncrement is the step/increment for IDENTITY columns.
	// Zero means "use the dialect default" (usually 1).
	IdentityIncrement int64 `json:"identityIncrement,omitempty"`

	// IdentityGeneration controls the GENERATED clause for identity columns:
	// "ALWAYS" or "BY DEFAULT".  PostgreSQL, Oracle, and DB2 support both.
	// Empty defaults to "ALWAYS" at generation time.
	IdentityGeneration IdentityGeneration `json:"identityGeneration,omitempty"`

	// SequenceName allows explicit binding to a named sequence (PostgreSQL, Oracle).
	// When empty, the generator uses auto-increment / identity syntax instead.
	SequenceName string `json:"sequenceName,omitempty"`

	// IsGenerated indicates the column is a generated (computed) column.
	IsGenerated bool `json:"isGenerated,omitempty"`
	// GenerationExpression is the SQL expression for a generated column (e.g. "price * quantity").
	GenerationExpression string `json:"generationExpression,omitempty"`
	// GenerationStorage controls whether the generated column is VIRTUAL or STORED.
	GenerationStorage GenerationStorage `json:"generationStorage,omitempty"`

	// Invisible hides the column from SELECT * and some metadata views
	// in dialects that support invisible/hidden columns (Oracle, MySQL 8+).
	Invisible bool `json:"invisible,omitempty"`

	// Dialect-specific column option groups.
	MySQL      *MySQLColumnOptions    `json:"mysqlColumn,omitempty"`
	TiDB       *TiDBColumnOptions     `json:"tidbColumn,omitempty"`
	PostgreSQL *PostgresColumnOptions `json:"psqlColumn,omitempty"`
	Oracle     *OracleColumnOptions   `json:"oracleColumn,omitempty"`
	MSSQL      *MSSQLColumnOptions    `json:"mssqlColumn,omitempty"`
	DB2        *DB2ColumnOptions      `json:"db2Column,omitempty"`
	SQLite     *SQLiteColumnOptions   `json:"sqliteColumn,omitempty"`
}

// MySQLColumnOptions contains MySQL-specific column-level options.
//
// These options cover NDB Cluster storage hints and HeatWave secondary
// engine attributes.
// TODO: move ColumnFormat and Storage to "enums".
type MySQLColumnOptions struct {
	// ColumnFormat sets the column storage format hint: "FIXED", "DYNAMIC", or "DEFAULT" (NDB Cluster).
	ColumnFormat string `json:"columnFormat,omitempty"`
	// Storage specifies the storage medium for the column: "DISK" or "MEMORY" (NDB Cluster).
	Storage string `json:"storage,omitempty"`
	// PrimaryEngineAttribute is an opaque JSON string passed to the primary storage engine (e.g., InnoDB).
	PrimaryEngineAttribute string `json:"primaryEngineAttribute,omitempty"`
	// SecondaryEngineAttribute is an opaque JSON string passed to the secondary engine for this column.
	SecondaryEngineAttribute string `json:"secondaryEngineAttribute,omitempty"`
}

// TiDBColumnOptions contains TiDB-specific column-level options.
type TiDBColumnOptions struct {
	// ShardBits is the number of bits used for shard ID in AUTO_RANDOM.
	// This is the first argument: AUTO_RANDOM(ShardBits) or AUTO_RANDOM(ShardBits, RangeBits).
	ShardBits uint64 `json:"shardBits,omitempty"`

	// RangeBits is the number of bits used for the incremental part (optional, second argument).
	// When nil, TiDB uses the default (64 - ShardBits - 1 for sign bit).
	RangeBits *uint64 `json:"rangeBits,omitempty"`
}

// OracleColumnOptions contains Oracle-specific column-level options.
//
// Oracle supports transparent data encryption (TDE) at the column level,
// invisible columns (hidden from SELECT *), and DEFAULT ON NULL for
// columns that should use the default value when NULL is explicitly inserted.
type OracleColumnOptions struct {
	// Encrypt enables Transparent Data Encryption (TDE) for this column.
	Encrypt bool `json:"encrypt,omitempty"`

	// EncryptionAlgorithm specifies the encryption algorithm (e.g., "AES256", "AES192", "AES128", "3DES168").
	// Only used when Encrypt is true.
	EncryptionAlgorithm string `json:"encryptionAlgorithm,omitempty"`

	// Salt controls whether the encrypted column uses SALT (true) or NO SALT (false).
	// nil means use Oracle's default (SALT enabled).
	// SALT adds random data to encryption, making identical values encrypt differently.
	Salt *bool `json:"salt,omitempty"`

	// DefaultOnNull causes the column to use its DEFAULT value when NULL is explicitly
	// inserted (Oracle 12c+). This is distinct from NOT NULL.
	DefaultOnNull bool `json:"defaultOnNull,omitempty"`
}

// MSSQLColumnOptions contains Microsoft SQL Server-specific column-level options.
//
// SQL Server supports specialized column storage (FILESTREAM, SPARSE),
// security features (Always Encrypted, Dynamic Data Masking), and
// replication/synchronization controls.
type MSSQLColumnOptions struct {
	// FileStream enables FILESTREAM storage for VARBINARY(MAX) columns,
	// storing data in the NTFS file system while maintaining transactional consistency.
	FileStream bool `json:"fileStream,omitempty"`

	// Sparse optimizes storage for columns that are mostly NULL.
	// NULL values consume no space, but non-NULL values have a small overhead.
	// Most effective when 40%+ of values are NULL.
	Sparse bool `json:"sparse,omitempty"`

	// RowGUIDCol marks a UNIQUEIDENTIFIER column as the row's GUID.
	// Used for merge replication and distributed scenarios.
	RowGUIDCol bool `json:"rowGuidCol,omitempty"`

	// IdentityNotForReplication prevents identity values from being
	// incremented during replication operations.
	IdentityNotForReplication bool `json:"identityNotForReplication,omitempty"`

	// Persisted stores a computed column's value physically in the table
	// (equivalent to STORED for generated columns). When false and IsGenerated
	// is true, the column is computed on-the-fly (VIRTUAL).
	Persisted bool `json:"persisted,omitempty"`

	// AlwaysEncrypted configures Always Encrypted for this column.
	AlwaysEncrypted *MSSQLAlwaysEncryptedOptions `json:"alwaysEncrypted,omitempty"`

	// DataMasking configures Dynamic Data Masking for this column.
	DataMasking *MSSQLDataMaskingOptions `json:"dataMasking,omitempty"`
}

// MSSQLAlwaysEncryptedOptions configures Always Encrypted column encryption.
//
// Always Encrypted protects sensitive data by encrypting it on the client side.
// The database engine never sees the plaintext data or encryption keys.
type MSSQLAlwaysEncryptedOptions struct {
	// ColumnEncryptionKey is the name of the column encryption key (CEK) to use.
	ColumnEncryptionKey string `json:"columnEncryptionKey,omitempty"`

	// EncryptionType specifies the encryption mode:
	// - "DETERMINISTIC": Same plaintext always encrypts to same ciphertext (allows equality searches, joins, grouping)
	// - "RANDOMIZED": Same plaintext encrypts to different ciphertext each time (more secure, but no operations allowed)
	EncryptionType string `json:"encryptionType,omitempty"`

	// Algorithm is the encryption algorithm, typically "AEAD_AES_256_CBC_HMAC_SHA_256".
	Algorithm string `json:"algorithm,omitempty"`
}

// MSSQLDataMaskingOptions configures Dynamic Data Masking.
//
// Dynamic Data Masking obfuscates sensitive data in query results for
// non-privileged users without changing the actual data in the database.
type MSSQLDataMaskingOptions struct {
	// Function is the masking function to apply:
	// - "default()": Full masking (XXXX for strings, 0 for numbers, 01-01-1900 for dates)
	// - "email()": Masks email addresses (aXXX @XXXX.com)
	// - "partial(prefix, padding, suffix)": Shows first/last N chars (e.g., "partial(1,\"XXXX\",2)")
	// - "random(start, end)": Replaces numeric values with random number in range
	Function string `json:"function,omitempty"`
}

// DB2ColumnOptions contains IBM DB2-specific column-level options.
//
// DB2 supports inline length specifications for LOB and structured types,
// column-level compression, and implicitly hidden columns.
type DB2ColumnOptions struct {
	// InlineLength specifies the maximum length (in bytes) stored inline
	// for LOB or structured type columns. Data exceeding this length is
	// stored separately.
	InlineLength *int `json:"inlineLength,omitempty"`

	// Compress enables compression for this column (LOB columns).
	// true = COMPRESS YES, false = COMPRESS NO, nil = use table default.
	Compress *bool `json:"compress,omitempty"`

	// ImplicitlyHidden marks the column as IMPLICITLY HIDDEN (DB2 10.1+).
	// Similar to Oracle's INVISIBLE: excluded from SELECT * but can be
	// explicitly referenced.
	ImplicitlyHidden bool `json:"implicitlyHidden,omitempty"`
}

// SQLiteColumnOptions contains SQLite-specific column-level options.
//
// SQLite's column options are minimal. The main distinguishing feature
// is the AUTOINCREMENT keyword which provides stricter guarantees than
// the default INTEGER PRIMARY KEY behavior.
type SQLiteColumnOptions struct {
	// StrictAutoincrement forces use of the AUTOINCREMENT keyword.
	// When true, generates "INTEGER PRIMARY KEY AUTOINCREMENT" instead of
	// just "INTEGER PRIMARY KEY".
	//
	// Differences:
	// - Regular: rowid values may be reused after DELETE
	// - AUTOINCREMENT: rowid values are strictly monotonic and never reused
	//
	// Trade-off: AUTOINCREMENT requires additional bookkeeping overhead.
	StrictAutoincrement bool `json:"strictAutoincrement,omitempty"`
}

// DataType is an ENUM with all possible column data types.
type DataType string

const (
	DataTypeString   DataType = "string"
	DataTypeInt      DataType = "int"
	DataTypeFloat    DataType = "float"
	DataTypeBoolean  DataType = "boolean"
	DataTypeDatetime DataType = "datetime"
	DataTypeJSON     DataType = "json"
	DataTypeUUID     DataType = "uuid"
	DataTypeBinary   DataType = "binary"
	DataTypeEnum     DataType = "enum"
	DataTypeUnknown  DataType = "unknown"
)

// GenerationStorage is an ENUM with all possible column generation storage options.
type GenerationStorage string

const (
	GenerationVirtual GenerationStorage = "VIRTUAL"
	GenerationStored  GenerationStorage = "STORED"
)

// Constraint represents a table-level constraint (PK, FK, UNIQUE, or CHECK).
type Constraint struct {
	// Name is the constraint identifier (auto-generated when omitted).
	Name string `json:"name,omitempty"`
	// Type is the constraint kind: PRIMARY KEY, FOREIGN KEY, UNIQUE, or CHECK.
	Type ConstraintType `json:"type"`
	// Columns list the column names that participate in this constraint.
	Columns []string `json:"columns"`

	// ReferencedTable is the target table for a FOREIGN KEY constraint.
	ReferencedTable string `json:"referencedTable,omitempty"`
	// ReferencedColumns lists the target columns in ReferencedTable for a FOREIGN KEY.
	ReferencedColumns []string `json:"referencedColumns,omitempty"`
	// OnDelete is the referential action executed when a referenced row is deleted.
	OnDelete ReferentialAction `json:"onDelete,omitempty"`
	// OnUpdate is the referential action executed when a referenced row is updated.
	OnUpdate ReferentialAction `json:"onUpdate,omitempty"`

	// CheckExpression is the SQL boolean expression for a CHECK constraint.
	CheckExpression string `json:"checkExpression,omitempty"`
	// Enforced controls whether a CHECK constraint is actively enforced (MySQL 8.0.16+).
	Enforced bool `json:"enforced,omitempty"`
}

// ConstraintType is an ENUM with all possible constraint types.
type ConstraintType string

const (
	ConstraintPrimaryKey ConstraintType = "PRIMARY KEY"
	ConstraintForeignKey ConstraintType = "FOREIGN KEY"
	ConstraintUnique     ConstraintType = "UNIQUE"
	ConstraintCheck      ConstraintType = "CHECK"
)

// ReferentialAction is an ENUM with all possible column references after action.
type ReferentialAction string

const (
	RefActionNone       ReferentialAction = ""
	RefActionCascade    ReferentialAction = "CASCADE"
	RefActionRestrict   ReferentialAction = "RESTRICT"
	RefActionSetNull    ReferentialAction = "SET NULL"
	RefActionSetDefault ReferentialAction = "SET DEFAULT"
	RefActionNoAction   ReferentialAction = "NO ACTION"
)

// Index represents a table index (B-Tree, Hash, Full-Text, Spatial, etc.).
type Index struct {
	// Name is the index identifier.
	Name string `json:"name,omitempty"`
	// Columns list the columns (with optional prefix length and sort order) covered by the index.
	Columns []ColumnIndex `json:"columns"`
	// Unique marks the index as a UNIQUE index that prevents duplicate values.
	Unique bool `json:"unique,omitempty"`
	// Type is the index algorithm or kind (BTREE, HASH, FULLTEXT, SPATIAL, GIN, GiST).
	Type IndexType `json:"type,omitempty"`
	// Comment is an optional descriptive comment stored with the index metadata.
	Comment string `json:"comment,omitempty"`
	// Visibility controls whether the optimizer considers this index (VISIBLE or INVISIBLE).
	Visibility IndexVisibility `json:"visibility,omitempty"`
}

// ColumnIndex describes a single column reference within an index definition.
type ColumnIndex struct {
	// Name is the column name included in the index.
	Name string `json:"name"`
	// Length is the prefix length in characters/bytes for partial-index support (0 = full column).
	Length int `json:"length,omitempty"`
	// Order is the sort direction for this column in the index (ASC or DESC).
	Order SortOrder `json:"order,omitempty"`
}

// IndexType is an ENUM with all possible index types.
type IndexType string

const (
	IndexTypeBTree    IndexType = "BTREE"
	IndexTypeHash     IndexType = "HASH"
	IndexTypeFullText IndexType = "FULLTEXT"
	IndexTypeSpatial  IndexType = "SPATIAL"
	IndexTypeGIN      IndexType = "GIN"
	IndexTypeGiST     IndexType = "GiST"
)

// IndexVisibility is an ENUM with all possible index visibilities.
type IndexVisibility string

const (
	IndexVisible   IndexVisibility = "VISIBLE"
	IndexInvisible IndexVisibility = "INVISIBLE"
)

// SortOrder is an ENUM with all possible column sort orders.
type SortOrder string

const (
	SortAsc  SortOrder = "ASC"
	SortDesc SortOrder = "DESC"
)

// FindTable looks for a table by name inside a database.
func (db *Database) FindTable(name string) *Table {
	if db == nil {
		return nil
	}
	for _, t := range db.Tables {
		if t.Name == name {
			return t
		}
	}
	return nil
}

// FindColumn looks for a column by name inside a table.
func (t *Table) FindColumn(name string) *Column {
	for _, c := range t.Columns {
		if c.Name == name {
			return c
		}
	}
	return nil
}

// FindConstraint looks for a constraint by name inside a table.
func (t *Table) FindConstraint(name string) *Constraint {
	for _, c := range t.Constraints {
		if c.Name == name {
			return c
		}
	}
	return nil
}

// FindIndex looks for an index by name inside a table.
func (t *Table) FindIndex(name string) *Index {
	for _, i := range t.Indexes {
		if i.Name == name {
			return i
		}
	}
	return nil
}

// PrimaryKey returns the primary key constraint of the table.
func (t *Table) PrimaryKey() *Constraint {
	for _, c := range t.Constraints {
		if c.Type == ConstraintPrimaryKey {
			return c
		}
	}
	return nil
}

// Names returns the names of the columns in the index.
func (i *Index) Names() []string {
	names := make([]string, len(i.Columns))
	for idx, col := range i.Columns {
		names[idx] = col.Name
	}
	return names
}

// String returns a string representation of a table with all columns, constraints, and indexes.
func (t *Table) String() string {
	return fmt.Sprintf("Table: %s (%d cols, %d constraints, %d indexes)",
		t.Name, len(t.Columns), len(t.Constraints), len(t.Indexes))
}

// HasIdentityOptions reports whether seed or increment are explicitly set.
func (c *Column) HasIdentityOptions() bool {
	return c.IdentitySeed != 0 || c.IdentityIncrement != 0
}

// ParseReferences splits a "table.column" reference string into its two parts.
// It returns ("", "", false) if the format is invalid.
func ParseReferences(ref string) (table, column string, ok bool) {
	ref = strings.TrimSpace(ref)
	dot := strings.LastIndex(ref, ".")
	if dot <= 0 || dot >= len(ref)-1 {
		return "", "", false
	}
	return ref[:dot], ref[dot+1:], true
}

type normalizeDataTypeRule struct {
	dataType   DataType
	substrings []string
}

var normalizeDataTypeRules = []normalizeDataTypeRule{
	{dataType: DataTypeEnum, substrings: []string{"enum"}},
	{dataType: DataTypeString, substrings: []string{"char", "text", "string", "set"}},
	{dataType: DataTypeBoolean, substrings: []string{"bool", "tinyint(1)"}},
	{dataType: DataTypeInt, substrings: []string{"int"}},
	{dataType: DataTypeFloat, substrings: []string{"float", "double", "decimal", "numeric", "real"}},
	{dataType: DataTypeDatetime, substrings: []string{"timestamp", "date", "time"}},
	{dataType: DataTypeJSON, substrings: []string{"json"}},
	{dataType: DataTypeUUID, substrings: []string{"uuid"}},
	{dataType: DataTypeBinary, substrings: []string{"blob", "binary", "varbinary"}},
}

// NormalizeDataType maps a raw SQL type string (e.g. "VARCHAR(255)") to one of
// the portable DataType constants. The matching is case-insensitive and based
// on substring containment using normalizeDataTypeRules.
func NormalizeDataType(rawType string) DataType {
	lower := strings.ToLower(strings.TrimSpace(rawType))
	for _, rule := range normalizeDataTypeRules {
		for _, sub := range rule.substrings {
			if strings.Contains(lower, sub) {
				return rule.dataType
			}
		}
	}
	return DataTypeUnknown
}

// AutoGenerateConstraintName produces a deterministic name for a constraint
// that was synthesized from column-level shortcuts.
//
//	PK:     pk_{table}
//	UNIQUE: uq_{table}_{column}
//	CHECK:  chk_{table}_{column}
//	FK:     fk_{table}_{referenced_table}
func AutoGenerateConstraintName(ctype ConstraintType, table string, columns []string, refTable string) string {
	t := strings.ToLower(table)
	switch ctype {
	case ConstraintPrimaryKey:
		return "pk_" + t
	case ConstraintUnique:
		return fmt.Sprintf("uq_%s_%s", t, strings.ToLower(strings.Join(columns, "_")))
	case ConstraintCheck:
		return fmt.Sprintf("chk_%s_%s", t, strings.ToLower(strings.Join(columns, "_")))
	case ConstraintForeignKey:
		return fmt.Sprintf("fk_%s_%s", t, strings.ToLower(refTable))
	default:
		return fmt.Sprintf("cstr_%s_%s", t, strings.ToLower(strings.Join(columns, "_")))
	}
}

// BuildEnumTypeRaw constructs a portable enum type string from a list of
// values, e.g. ["free","pro"] -> "enum('free','pro')".
func BuildEnumTypeRaw(values []string) string {
	if len(values) == 0 {
		return "enum()"
	}
	var sb strings.Builder
	sb.Grow(len(values) * 8)
	sb.WriteString("enum(")
	for i, v := range values {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteByte('\'')
		sb.WriteString(strings.ReplaceAll(v, "'", "''"))
		sb.WriteByte('\'')
	}
	sb.WriteByte(')')
	return sb.String()
}
