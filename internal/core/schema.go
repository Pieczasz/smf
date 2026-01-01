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
	Options     string
}

type Column struct {
	Name          string
	TypeRaw       string
	Type          string
	Nullable      bool
	PrimaryKey    bool
	AutoIncrement bool
	DefaultValue  *string
	Comment       string
}

type Constraint struct {
	Name    string
	Type    ConstraintType
	Columns []string

	ReferencedTable  string
	ReferencedColumn string
	OnDelete         string
	OnUpdate         string

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
