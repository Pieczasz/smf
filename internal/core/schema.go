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
	Name          string
	Columns       []*Column
	Constraints   []*Constraint
	Indexes       []*Index
	Comment       string
	Options       string
	Charset       string
	Collate       string
	Engine        string
	AutoIncrement uint64

	RowFormat        string
	AvgRowLength     uint64
	KeyBlockSize     uint64
	MaxRows          uint64
	MinRows          uint64
	Checksum         uint64
	DelayKeyWrite    uint64
	Tablespace       string
	Compression      string
	Encryption       string
	PackKeys         string
	DataDirectory    string
	IndexDirectory   string
	InsertMethod     string
	StorageMedia     string
	StatsPersistent  string
	StatsAutoRecalc  string
	StatsSamplePages string
	Union            []string

	AutoIdCache              uint64
	AutoRandomBase           uint64
	ShardRowID               uint64
	PreSplitRegion           uint64
	Connection               string
	Password                 string
	Nodegroup                uint64
	SecondaryEngine          string
	TableChecksum            uint64
	TTL                      string
	TTLEnable                bool
	TTLJobInterval           string
	EngineAttribute          string
	SecondaryEngineAttribute string
	AutoextendSize           string
	PageChecksum             uint64
	PageCompressed           bool
	PageCompressionLevel     uint64
	Transactional            uint64
	IetfQuotes               bool
	Sequence                 bool
	Affinity                 string
	PlacementPolicy          string
	StatsBuckets             uint64
	StatsTopN                uint64
	StatsColsChoice          string
	StatsColList             string
	StatsSampleRate          float64
}

type Column struct {
	Name          string
	TypeRaw       string
	Type          string
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
	GenerationStorage    string

	ColumnFormat string
	Storage      string
	AutoRandom   uint64

	SecondaryEngineAttribute string
}

type Constraint struct {
	Name    string
	Type    ConstraintType
	Columns []string

	ReferencedTable   string
	ReferencedColumns []string
	OnDelete          string
	OnUpdate          string

	CheckExpression string
}

type ConstraintType string

const (
	PrimaryKey ConstraintType = "PRIMARY KEY"
	ForeignKey ConstraintType = "FOREIGN KEY"
	Unique     ConstraintType = "UNIQUE"
	Check      ConstraintType = "CHECK"
)

type Index struct {
	Name    string
	Columns []string
	Unique  bool
	Type    string
}

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

func (t *Table) String() string {
	return fmt.Sprintf("Table: %s (%d cols, %d constraints)", t.Name, len(t.Columns), len(t.Constraints))
}
