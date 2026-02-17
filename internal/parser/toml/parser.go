// Package toml provides a parser for the smf TOML schema format.
// It reads a dialect-agnostic schema definition from a .toml file and
// converts it into the canonical core.Database representation that the
// rest of the smf toolchain operates on.
package toml

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/BurntSushi/toml"

	"smf/internal/core"
)

// schemaFile is the top-level TOML document.
// In the new schema format, [database], [validation], and [[tables]]
// are all top-level keys (tables and validation are NOT nested under a database).
type schemaFile struct {
	Database   tomlDatabase    `toml:"database"`
	Validation *tomlValidation `toml:"validation"`
	Tables     []tomlTable     `toml:"tables"`
}

// tomlDatabase maps [database].
type tomlDatabase struct {
	Name    string `toml:"name"`
	Dialect string `toml:"dialect"`
}

// tomlValidation maps [validation].
type tomlValidation struct {
	MaxTableNameLength          int    `toml:"max_table_name_length"`
	MaxColumnNameLength         int    `toml:"max_column_name_length"`
	AutoGenerateConstraintNames bool   `toml:"auto_generate_constraint_names"`
	AllowedNamePattern          string `toml:"allowed_name_pattern"`
}

// Parser reads smf TOML schema files.
type Parser struct{}

// NewParser creates a new TOML schema parser.
func NewParser() *Parser {
	return &Parser{}
}

// ParseFile opens the file at the given path and parses it as a TOML schema.
func (p *Parser) ParseFile(path string) (*core.Database, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("toml: open file %q: %w", path, err)
	}
	defer f.Close()

	return p.Parse(f)
}

// Parse reads TOML content from the reader and returns the corresponding core.Database.
func (p *Parser) Parse(r io.Reader) (*core.Database, error) {
	var sf schemaFile
	if _, err := toml.NewDecoder(r).Decode(&sf); err != nil {
		return nil, fmt.Errorf("toml: decode error: %w", err)
	}

	db, err := newConverter(&sf).convert()
	if err != nil {
		return nil, err
	}

	if err := db.Validate(); err != nil {
		return nil, fmt.Errorf("toml: %w", err)
	}

	return db, nil
}

type converter struct {
	sf      *schemaFile
	dialect *core.Dialect
}

func newConverter(sf *schemaFile) *converter {
	return &converter{sf: sf}
}

func (c *converter) convert() (*core.Database, error) {
	c.dialect = new(core.Dialect(strings.ToLower(c.sf.Database.Dialect)))
	db := &core.Database{
		Name:    c.sf.Database.Name,
		Dialect: c.dialect,
		Tables:  make([]*core.Table, 0, len(c.sf.Tables)),
	}
	db.Validation = convertRules(c.sf.Validation)

	for i := range c.sf.Tables {
		t, err := c.convertTable(&c.sf.Tables[i])
		if err != nil {
			return nil, fmt.Errorf("toml: table %q: %w", c.sf.Tables[i].Name, err)
		}
		db.Tables = append(db.Tables, t)
	}

	return db, nil
}

// convertRules converts [validation] into core.ValidationRules.
// No validation is performed here — that happens in db.Validate().
func convertRules(v *tomlValidation) *core.ValidationRules {
	if v == nil {
		return nil
	}
	return &core.ValidationRules{
		MaxTableNameLength:          v.MaxTableNameLength,
		MaxColumnNameLength:         v.MaxColumnNameLength,
		AutoGenerateConstraintNames: v.AutoGenerateConstraintNames,
		AllowedNamePattern:          v.AllowedNamePattern,
	}
}
