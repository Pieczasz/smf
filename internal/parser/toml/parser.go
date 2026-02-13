// Package toml provides a parser for the smf TOML schema format.
// It reads a dialect-agnostic schema definition from a .toml file and
// converts it into the canonical core.Database representation that the
// rest of the smf toolchain operates on.
package toml

import (
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"

	"github.com/BurntSushi/toml"

	"smf/internal/core"
)

// TODO: validate enum string values e.g. constraint type = "BANANA"`, `on_delete = "OOPS"`, `order = "SIDEWAYS"`, etc.
// validate Cross-table FK target existence — `references = "users.id"` is syntactically validated, but we don't verify that a table named `users` with column `id` exists in this schema
// validate Dialect-specific semantic rules auto_increment` on a non-integer column, `nullable = true` on a PK column, generated column without expression, etc.
// NOTE: currently empty schema is allowed, and empty database name too.

// schemaFile is the top-level TOML document.
// In the new schema format, [database], [validation], and [[tables]]
// are all top-level keys (tables and validation are NOT nested under database).
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

// Parse reads TOML content from reader and returns the corresponding core.Database.
func (p *Parser) Parse(r io.Reader) (*core.Database, error) {
	var sf schemaFile
	if _, err := toml.NewDecoder(r).Decode(&sf); err != nil {
		return nil, fmt.Errorf("toml: decode error: %w", err)
	}

	return newConverter(&sf).convert()
}

type converter struct {
	sf         *schemaFile
	dialect    *core.Dialect
	rules      *core.ValidationRules
	nameRe     *regexp.Regexp
	seenTables map[string]bool
}

func newConverter(sf *schemaFile) *converter {
	return &converter{
		sf:         sf,
		seenTables: make(map[string]bool, len(sf.Tables)),
	}
}

func (c *converter) convert() (*core.Database, error) {
	dialect, err := validateDialect(c.sf.Database.Dialect)
	if err != nil {
		return nil, err
	}
	c.dialect = dialect

	if err := c.validateRules(); err != nil {
		return nil, err
	}

	db := &core.Database{
		Name:    c.sf.Database.Name,
		Dialect: c.dialect,
		Tables:  make([]*core.Table, 0, len(c.sf.Tables)),
	}
	db.Validation = c.rules

	for i := range c.sf.Tables {
		t, err := c.convertTable(&c.sf.Tables[i])
		if err != nil {
			return nil, fmt.Errorf("toml: table %q: %w", c.sf.Tables[i].Name, err)
		}
		db.Tables = append(db.Tables, t)
	}

	return db, nil
}

// validateDialect validates the raw dialect string.
// Empty is allowed (dialect is optional); an unrecognized non-empty value is an error.
func validateDialect(raw string) (*core.Dialect, error) {
	if raw == "" {
		return nil, nil
	}
	if !core.IsValidDialect(raw) {
		return nil, fmt.Errorf("toml: unsupported dialect %q; supported: %v", raw, core.SupportedDialects())
	}
	d := core.Dialect(strings.ToLower(raw))
	return &d, nil
}

// validateRules converts [validation] and pre-compiles the name regex.
func (c *converter) validateRules() error {
	v := c.sf.Validation
	if v == nil {
		return nil
	}

	c.rules = &core.ValidationRules{
		MaxTableNameLength:          v.MaxTableNameLength,
		MaxColumnNameLength:         v.MaxColumnNameLength,
		AutoGenerateConstraintNames: v.AutoGenerateConstraintNames,
		AllowedNamePattern:          v.AllowedNamePattern,
	}

	if v.AllowedNamePattern != "" {
		re, err := regexp.Compile(v.AllowedNamePattern)
		if err != nil {
			return fmt.Errorf("toml: invalid allowed_name_pattern %q: %w", v.AllowedNamePattern, err)
		}
		c.nameRe = re
	}

	return nil
}
