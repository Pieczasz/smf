package core

import (
	"fmt"
	"strings"
)

type Database struct {
	Name   string
	Tables []*Table
}

type Table struct {
	Name        string
	Columns     []*Column
	Constraints []*Constraint
	Indexes     []*Index
	Comment     string
	Options     TableOptions
}

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

type TiDBTableOptions struct {
	AutoIdCache     uint64
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

type Column struct {
	Name          string
	TypeRaw       string
	Type          DataType
	Nullable      bool
	PrimaryKey    bool
	AutoIncrement bool
	DefaultValue  *string
	OnUpdate      *string
	Comment       string
	Collate       string
	Charset       string

	IsGenerated          bool
	GenerationExpression string
	GenerationStorage    GenerationStorage

	ColumnFormat             string
	Storage                  string
	AutoRandom               uint64
	SecondaryEngineAttribute string
}

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

type GenerationStorage string

const (
	GenerationVirtual GenerationStorage = "VIRTUAL"
	GenerationStored  GenerationStorage = "STORED"
)

type Constraint struct {
	Name    string
	Type    ConstraintType
	Columns []string

	ReferencedTable   string
	ReferencedColumns []string
	OnDelete          ReferentialAction
	OnUpdate          ReferentialAction

	CheckExpression string
	Enforced        bool
}

type ConstraintType string

const (
	ConstraintPrimaryKey ConstraintType = "PRIMARY KEY"
	ConstraintForeignKey ConstraintType = "FOREIGN KEY"
	ConstraintUnique     ConstraintType = "UNIQUE"
	ConstraintCheck      ConstraintType = "CHECK"
)

type ReferentialAction string

const (
	RefActionNone       ReferentialAction = ""
	RefActionCascade    ReferentialAction = "CASCADE"
	RefActionRestrict   ReferentialAction = "RESTRICT"
	RefActionSetNull    ReferentialAction = "SET NULL"
	RefActionSetDefault ReferentialAction = "SET DEFAULT"
	RefActionNoAction   ReferentialAction = "NO ACTION"
)

type Index struct {
	Name       string
	Columns    []IndexColumn
	Unique     bool
	Type       IndexType
	Comment    string
	Visibility IndexVisibility
}

type IndexColumn struct {
	Name   string
	Length int
	Order  SortOrder
}

type SortOrder string

const (
	SortAsc  SortOrder = "ASC"
	SortDesc SortOrder = "DESC"
)

type IndexType string

const (
	IndexTypeBTree    IndexType = "BTREE"
	IndexTypeHash     IndexType = "HASH"
	IndexTypeFullText IndexType = "FULLTEXT"
	IndexTypeSpatial  IndexType = "SPATIAL"
	IndexTypeGIN      IndexType = "GIN"
	IndexTypeGiST     IndexType = "GiST"
)

type IndexVisibility string

const (
	IndexVisible   IndexVisibility = "VISIBLE"
	IndexInvisible IndexVisibility = "INVISIBLE"
)

func (db *Database) FindTable(name string) *Table {
	for _, t := range db.Tables {
		if strings.EqualFold(t.Name, name) {
			return t
		}
	}
	return nil
}

func (t *Table) FindColumn(name string) *Column {
	for _, c := range t.Columns {
		if strings.EqualFold(c.Name, name) {
			return c
		}
	}
	return nil
}

func (t *Table) FindConstraint(name string) *Constraint {
	for _, c := range t.Constraints {
		if strings.EqualFold(c.Name, name) {
			return c
		}
	}
	return nil
}

func (t *Table) FindIndex(name string) *Index {
	for _, i := range t.Indexes {
		if strings.EqualFold(i.Name, name) {
			return i
		}
	}
	return nil
}

func (t *Table) PrimaryKey() *Constraint {
	for _, c := range t.Constraints {
		if c.Type == ConstraintPrimaryKey {
			return c
		}
	}
	return nil
}

func (i *Index) ColumnNames() []string {
	names := make([]string, len(i.Columns))
	for idx, col := range i.Columns {
		names[idx] = col.Name
	}
	return names
}

func (t *Table) String() string {
	return fmt.Sprintf("Table: %s (%d cols, %d constraints, %d indexes)",
		t.Name, len(t.Columns), len(t.Constraints), len(t.Indexes))
}

func NormalizeDataType(rawType string) DataType {
	rawType = strings.ToLower(strings.TrimSpace(rawType))

	switch {
	case containsAny(rawType, "char", "text", "string", "enum", "set"):
		return DataTypeString
	case strings.Contains(rawType, "bool") || strings.Contains(rawType, "tinyint(1)"):
		return DataTypeBoolean
	case strings.Contains(rawType, "int"):
		return DataTypeInt
	case containsAny(rawType, "float", "double", "decimal", "numeric", "real"):
		return DataTypeFloat
	case strings.Contains(rawType, "timestamp"):
		return DataTypeDatetime
	case containsAny(rawType, "date", "time"):
		return DataTypeDatetime
	case strings.Contains(rawType, "json"):
		return DataTypeJSON
	case strings.Contains(rawType, "uuid"):
		return DataTypeUUID
	case containsAny(rawType, "blob", "binary", "varbinary"):
		return DataTypeBinary
	default:
		return DataTypeUnknown
	}
}

func containsAny(s string, substrs ...string) bool {
	for _, sub := range substrs {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}
