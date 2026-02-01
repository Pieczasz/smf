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
	Name   string
	Tables []*Table
}

// Table represents a table in the schema.
type Table struct {
	Name        string        `json:"name"`
	Columns     []*Column     `json:"columns"`
	Constraints []*Constraint `json:"constraints,omitempty"`
	Indexes     []*Index      `json:"indexes,omitempty"`
	Comment     string        `json:"comment,omitempty"`
	Options     TableOptions  `json:"options"`
}

// TableOptions represents the options for a table in the schema.
// TODO: split this big struct into several smaller ones.
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

	MySQL MySQLTableOptions
	TiDB  TiDBTableOptions
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

// TiDBTableOptions contains TiDB-specific table options. Since we use TIDB mysql parser,
// it is a nice addition to mysql parser.
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

// Column represents a single column inside schema
type Column struct {
	Name          string   `json:"name"`
	TypeRaw       string   `json:"typeRaw"`
	Type          DataType `json:"type"`
	Nullable      bool     `json:"nullable"`
	PrimaryKey    bool     `json:"primaryKey"`
	AutoIncrement bool     `json:"autoIncrement"`
	DefaultValue  *string  `json:"defaultValue,omitempty"`
	OnUpdate      *string  `json:"onUpdate,omitempty"`
	Comment       string   `json:"comment,omitempty"`
	Collate       string   `json:"collate,omitempty"`
	Charset       string   `json:"charset,omitempty"`

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

type normalizeDataTypeRule struct {
	dataType   DataType
	substrings []string
}

var normalizeDataTypeRules = []normalizeDataTypeRule{
	{dataType: DataTypeString, substrings: []string{"char", "text", "string", "enum", "set"}},
	{dataType: DataTypeBoolean, substrings: []string{"bool", "tinyint(1)"}},
	{dataType: DataTypeInt, substrings: []string{"int"}},
	{dataType: DataTypeFloat, substrings: []string{"float", "double", "decimal", "numeric", "real"}},
	{dataType: DataTypeDatetime, substrings: []string{"timestamp", "date", "time"}},
	{dataType: DataTypeJSON, substrings: []string{"json"}},
	{dataType: DataTypeUUID, substrings: []string{"uuid"}},
	{dataType: DataTypeBinary, substrings: []string{"blob", "binary", "varbinary"}},
}

// NormalizeDataType normalizes a raw data type string to a DataType.
func NormalizeDataType(rawType string) DataType {
	normalized := strings.ToLower(strings.TrimSpace(rawType))
	for _, rule := range normalizeDataTypeRules {
		if containsAny(normalized, rule.substrings...) {
			return rule.dataType
		}
	}
	return DataTypeUnknown
}

func containsAny(s string, substrs ...string) bool {
	for _, sub := range substrs {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}
