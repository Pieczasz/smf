// Package core contains the single source of truth from the database schema.
// It provides a structured representation of data for tables, columns, constraints, and so on
// for all databases that we support.
package core

import (
	"fmt"
	"strings"
)

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
	}
}

// IsValidDialect reports whether d is a recognized dialect string.
func IsValidDialect(d string) bool {
	for _, supported := range SupportedDialects() {
		if strings.EqualFold(string(supported), d) {
			return true
		}
	}
	return false
}

// Database represents a database in the schema.
type Database struct {
	Name       string
	Dialect    string
	Version    string
	Tables     []*Table
	Validation *ValidationRules
}

// ValidationRules configures schema-level validation constraints.
type ValidationRules struct {
	MaxTableNameLength          int    `json:"maxTableNameLength,omitempty"`
	MaxColumnNameLength         int    `json:"maxColumnNameLength,omitempty"`
	AutoGenerateConstraintNames bool   `json:"autoGenerateConstraintNames,omitempty"`
	AllowedNamePattern          string `json:"allowedNamePattern,omitempty"` // Regex pattern for identifiers.
}

// Table represents a table in the schema.
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

// TableOptions represents the options for a table in the schema.
type TableOptions struct {
	Engine        string
	Charset       string
	Collate       string
	AutoIncrement uint64

	RowFormat      string
	AvgRowLength   uint64
	KeyBlockSize   uint64
	MaxRows        uint64
	MinRows        uint64
	Checksum       uint64
	DelayKeyWrite  uint64
	Tablespace     string
	Compression    string
	Encryption     string
	PackKeys       string
	DataDirectory  string
	IndexDirectory string
	InsertMethod   string
	StorageMedia   string

	StatsPersistent  string
	StatsAutoRecalc  string
	StatsSamplePages string

	Connection string
	Password   string

	AutoextendSize string
	PageChecksum   uint64
	Transactional  uint64

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

// MySQLTableOptions contains MySQL-specific table options.
type MySQLTableOptions struct {
	Union                    []string
	SecondaryEngine          string
	TableChecksum            uint64
	EngineAttribute          string
	SecondaryEngineAttribute string
	PageCompressed           bool
	PageCompressionLevel     uint64
	IetfQuotes               bool
	Nodegroup                uint64
}

// TiDBTableOptions contains TiDB-specific table options.
type TiDBTableOptions struct {
	AutoIDCache     uint64
	AutoRandomBase  uint64
	ShardRowID      uint64
	PreSplitRegion  uint64
	TTL             string
	TTLEnable       bool
	TTLJobInterval  string
	Affinity        string
	PlacementPolicy string
	StatsBuckets    uint64
	StatsTopN       uint64
	StatsColsChoice string
	StatsColList    string
	StatsSampleRate float64
	Sequence        bool
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
// MariaDB diverges from MySQL with its own encryption key management,
// Aria-engine options, and sequence objects.
type MariaDBTableOptions struct {
	// EncryptionKeyID specifies the encryption key ID for table encryption.
	EncryptionKeyID *int `json:"encryption_key_id,omitempty"`
	// Sequence marks the table as a SEQUENCE object (MariaDB 10.3+).
	Sequence bool `json:"sequence,omitempty"`
	// WithSystemVersioning enables system-versioned (temporal) table.
	WithSystemVersioning bool `json:"with_system_versioning,omitempty"`
}

// Column represents a single column inside schema.
type Column struct {
	Name          string   `json:"name"`
	TypeRaw       string   `json:"typeRaw"`
	Type          DataType `json:"type"`
	Nullable      bool     `json:"nullable"`
	PrimaryKey    bool     `json:"primaryKey"`
	AutoIncrement bool     `json:"autoIncrement"`
	DefaultValue  *string  `json:"defaultValue,omitempty"`
	OnUpdate      *string  `json:"onUpdate,omitempty"` // MySQL ON UPDATE CURRENT_TIMESTAMP
	Comment       string   `json:"comment,omitempty"`
	Collate       string   `json:"collate,omitempty"`
	Charset       string   `json:"charset,omitempty"`

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

	// RawType is the dialect-specific type override (e.g. "JSONB").
	// When set, it applies to the dialect declared in [database].
	// For all other dialects the portable TypeRaw is used.
	RawType string `json:"rawType,omitempty"`

	// RawTypeDialect is the dialect the RawType applies to.
	// Set automatically by the parser from [database].dialect.
	RawTypeDialect string `json:"rawTypeDialect,omitempty"`

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
	IdentityGeneration string `json:"identityGeneration,omitempty"`

	// SequenceName allows explicit binding to a named sequence (PostgreSQL, Oracle).
	// When empty, the generator uses auto-increment / identity syntax instead.
	SequenceName string `json:"sequenceName,omitempty"`

	IsGenerated          bool              `json:"isGenerated,omitempty"`
	GenerationExpression string            `json:"generationExpression,omitempty"`
	GenerationStorage    GenerationStorage `json:"generationStorage,omitempty"`

	ColumnFormat             string `json:"columnFormat,omitempty"`
	Storage                  string `json:"storage,omitempty"`
	AutoRandom               uint64 `json:"autoRandom,omitempty"`
	SecondaryEngineAttribute string `json:"secondaryEngineAttribute,omitempty"`
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

// Constraint contains all constraint options for a column.
type Constraint struct {
	Name    string         `json:"name,omitempty"`
	Type    ConstraintType `json:"type"`
	Columns []string       `json:"columns"`

	ReferencedTable   string            `json:"referencedTable,omitempty"`
	ReferencedColumns []string          `json:"referencedColumns,omitempty"`
	OnDelete          ReferentialAction `json:"onDelete,omitempty"`
	OnUpdate          ReferentialAction `json:"onUpdate,omitempty"`

	CheckExpression string `json:"checkExpression,omitempty"`
	Enforced        bool   `json:"enforced,omitempty"`
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

// Index contains all possible index options for a column.
type Index struct {
	Name       string          `json:"name,omitempty"`
	Columns    []IndexColumn   `json:"columns"`
	Unique     bool            `json:"unique,omitempty"`
	Type       IndexType       `json:"type,omitempty"`
	Comment    string          `json:"comment,omitempty"`
	Visibility IndexVisibility `json:"visibility,omitempty"`
}

// IndexColumn connects all column indexes.
type IndexColumn struct {
	Name   string    `json:"name"`
	Length int       `json:"length,omitempty"`
	Order  SortOrder `json:"order,omitempty"`
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

// GetName methods allow these types to be used with generic Named interface.
func (t *Table) GetName() string      { return t.Name }
func (c *Column) GetName() string     { return c.Name }
func (c *Constraint) GetName() string { return c.Name }
func (i *Index) GetName() string      { return i.Name }

// FindTable looks for a table by name inside a database.
func (db *Database) FindTable(name string) *Table {
	for _, t := range db.Tables {
		if strings.EqualFold(t.Name, name) {
			return t
		}
	}
	return nil
}

// FindColumn looks for a column by name inside a table.
func (t *Table) FindColumn(name string) *Column {
	for _, c := range t.Columns {
		if strings.EqualFold(c.Name, name) {
			return c
		}
	}
	return nil
}

// FindConstraint looks for a constraint by name inside a table.
func (t *Table) FindConstraint(name string) *Constraint {
	for _, c := range t.Constraints {
		if strings.EqualFold(c.Name, name) {
			return c
		}
	}
	return nil
}

// FindIndex looks for an index by name inside a table.
func (t *Table) FindIndex(name string) *Index {
	for _, i := range t.Indexes {
		if strings.EqualFold(i.Name, name) {
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

// HasTypeOverride reports whether the column has a type override for the given
// dialect.  When the dialect is empty, it returns true if ANY override exists.
func (c *Column) HasTypeOverride(dialect string) bool {
	if c.RawType == "" || strings.TrimSpace(c.RawType) == "" {
		return false
	}
	if dialect == "" {
		return true
	}
	return strings.EqualFold(c.RawTypeDialect, dialect)
}

// EffectiveType returns the type string a generator should use for the given
// dialect.  If the column has a raw type override matching the dialect, it is
// returned verbatim; otherwise TypeRaw (the portable type) is returned.
func (c *Column) EffectiveType(dialect string) string {
	if dialect != "" && c.RawType != "" && strings.TrimSpace(c.RawType) != "" && strings.EqualFold(c.RawTypeDialect, dialect) {
		return c.RawType
	}
	return c.TypeRaw
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
	if lower == "" {
		return DataTypeUnknown
	}
	for _, rule := range normalizeDataTypeRules {
		for _, sub := range rule.substrings {
			if strings.Contains(lower, sub) {
				return rule.dataType
			}
		}
	}
	return DataTypeUnknown
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
