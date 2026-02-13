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
	Engine       string `toml:"engine"`
	Charset      string `toml:"charset"`
	Collate      string `toml:"collate"`
	RowFormat    string `toml:"row_format"`
	Tablespace   string `toml:"tablespace"`
	Compression  string `toml:"compression"`
	Encryption   string `toml:"encryption"`
	KeyBlockSize uint64 `toml:"key_block_size"`
}

// tomlColumn maps [[tables.columns]].
type tomlColumn struct {
	Name          string `toml:"name"`
	Type          string `toml:"type"`
	PrimaryKey    bool   `toml:"primary_key"`
	AutoIncrement bool   `toml:"auto_increment"`
	Nullable      bool   `toml:"nullable"`
	Comment       string `toml:"comment"`
	Collate       string `toml:"collate"`
	Charset       string `toml:"charset"`

	// DefaultValue accepts string, bool, or number from TOML.
	// The converter normalizes everything to a string.
	// In the new schema this is the `default` key (was `default_value`).
	DefaultValue any `toml:"default"`

	// OnUpdate is used for MySQL ON UPDATE CURRENT_TIMESTAMP when there is
	// no inline FK (references is empty).  When references IS set,
	// on_update is treated as a referential action (CASCADE, RESTRICT, …).
	OnUpdate string `toml:"on_update"`
	OnDelete string `toml:"on_delete"`

	Unique     bool     `toml:"unique"`
	Check      string   `toml:"check"`
	References string   `toml:"references"`
	EnumValues []string `toml:"values"`

	// RawType is a single dialect-specific type override string.
	// When set, it applies to the dialect declared in [database].
	// For all other dialects the portable `type` value is used.
	// This replaces the old `type_overrides` map.
	RawType string `toml:"raw_type"`

	IsGenerated          bool   `toml:"is_generated"`
	GenerationExpression string `toml:"generation_expression"`
	GenerationStorage    string `toml:"generation_storage"` // "VIRTUAL" or "STORED"
}

// tomlConstraint maps [[tables.constraints]].
type tomlConstraint struct {
	Name              string   `toml:"name"`
	Type              string   `toml:"type"`
	Columns           []string `toml:"columns"`
	ReferencedTable   string   `toml:"referenced_table"`
	ReferencedColumns []string `toml:"referenced_columns"`
	OnDelete          string   `toml:"on_delete"`
	OnUpdate          string   `toml:"on_update"`
	CheckExpression   string   `toml:"check_expression"`
	Enforced          *bool    `toml:"enforced"` // pointer: absent -> true
}

// tomlIndex maps [[tables.indexes]].
type tomlIndex struct {
	Name       string `toml:"name"`
	Unique     bool   `toml:"unique"`
	Type       string `toml:"type"`
	Comment    string `toml:"comment"`
	Visibility string `toml:"visibility"`

	// Simple form:  columns = ["tenant_id", "created_at"]
	Columns []string `toml:"columns"`

	// Advanced form: [[tables.indexes.column_defs]]
	ColumnDefs []tomlColumnIndex `toml:"column_defs"`
}

// tomlColumnIndex maps [[tables.indexes.column_defs]].
type tomlColumnIndex struct {
	Name   string `toml:"name"`
	Length int    `toml:"length"`
	Order  string `toml:"order"`
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
