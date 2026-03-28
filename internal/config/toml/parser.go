// Package config/toml provides a parser for the smf TOML schema format.
// It reads a dialect-agnostic schema definition from a .toml file and
// converts it into the canonical schema.Database representation that the
// rest of the smf toolchain operates on.
package toml

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/BurntSushi/toml"

	"smf/internal/schema"
	"smf/internal/validate"
)

// schemaFile is the top-level TOML document.
// In the schema format, [database], [validation], and [[tables]]
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
// NOTE: config requires table and column names to be in snake_case .
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
func (p *Parser) ParseFile(path string) (*schema.Database, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("toml: couldn't open file %q: %w", path, err)
	}
	defer f.Close()

	return p.Parse(f)
}

// maxSchemaSize is the maximum allowed schema file size (10 MiB).
// TODO: maybe we can improve this limit somehow to read bigger files too.
const maxSchemaSize = 10 << 20

// Parse reads TOML content from the reader and returns the corresponding schema.Database.
func (p *Parser) Parse(r io.Reader) (*schema.Database, error) {
	var sf schemaFile
	if _, err := toml.NewDecoder(io.LimitReader(r, maxSchemaSize)).Decode(&sf); err != nil {
		return nil, fmt.Errorf("toml: decode error: %w", err)
	}

	db := &schema.Database{
		Name:    sf.Database.Name,
		Dialect: schema.Dialect(strings.ToLower(sf.Database.Dialect)),
		Tables:  make([]*schema.Table, 0, len(sf.Tables)),
	}
	db.Validation = rules(sf.Validation)

	for i := range sf.Tables {
		t, err := p.table(&sf.Tables[i], i)
		if err != nil {
			return nil, fmt.Errorf("toml: table %d (%q): %w", i, sf.Tables[i].Name, err)
		}
		db.Tables = append(db.Tables, t)
	}

	if err := validate.Database(db); err != nil {
		return nil, fmt.Errorf("toml: validate database: %w", err)
	}

	return db, nil
}

// rules parses [validation] into schema.ValidationRules.
func rules(v *tomlValidation) *schema.ValidationRules {
	if v == nil {
		return &schema.ValidationRules{}
	}
	return &schema.ValidationRules{
		MaxTableNameLength:          v.MaxTableNameLength,
		MaxColumnNameLength:         v.MaxColumnNameLength,
		AutoGenerateConstraintNames: v.AutoGenerateConstraintNames,
		AllowedNamePattern:          v.AllowedNamePattern,
	}
}
