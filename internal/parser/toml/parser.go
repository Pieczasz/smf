// Package toml provides a parser for the smf TOML schema format.
// It reads a dialect-agnostic schema definition from a .toml file and
// converts it into the canonical core.Database representation that the
// rest of the smf toolchain operates on.
package toml

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/BurntSushi/toml"

	"smf/internal/core"
)

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
	Version string `toml:"version"`
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
	// The converter normalises everything to a string.
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
	ColumnDefs []tomlIndexColumn `toml:"column_defs"`
}

// tomlIndexColumn maps [[tables.indexes.column_defs]].
type tomlIndexColumn struct {
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

// Parse reads TOML content from r and returns the corresponding core.Database.
func (p *Parser) Parse(r io.Reader) (*core.Database, error) {
	var sf schemaFile
	if _, err := toml.NewDecoder(r).Decode(&sf); err != nil {
		return nil, fmt.Errorf("toml: decode error: %w", err)
	}
	return convertSchemaFile(&sf)
}

func convertSchemaFile(sf *schemaFile) (*core.Database, error) {
	db := &core.Database{
		Name:    sf.Database.Name,
		Dialect: sf.Database.Dialect,
		Version: sf.Database.Version,
		Tables:  make([]*core.Table, 0, len(sf.Tables)),
	}

	if sf.Validation != nil {
		db.Validation = &core.ValidationRules{
			MaxTableNameLength:          sf.Validation.MaxTableNameLength,
			MaxColumnNameLength:         sf.Validation.MaxColumnNameLength,
			AutoGenerateConstraintNames: sf.Validation.AutoGenerateConstraintNames,
			AllowedNamePattern:          sf.Validation.AllowedNamePattern,
		}
	}

	dialect := sf.Database.Dialect

	for i := range sf.Tables {
		t, err := convertTable(&sf.Tables[i], dialect)
		if err != nil {
			return nil, fmt.Errorf("toml: table %q: %w", sf.Tables[i].Name, err)
		}
		db.Tables = append(db.Tables, t)
	}

	if err := db.Validate(); err != nil {
		return nil, fmt.Errorf("toml: schema validation failed: %w", err)
	}

	return db, nil
}

func convertTable(tt *tomlTable, dialect string) (*core.Table, error) {
	table := &core.Table{
		Name:    tt.Name,
		Comment: tt.Comment,
		Options: convertTableOptions(&tt.Options),
	}

	if tt.Timestamps != nil {
		table.Timestamps = &core.TimestampsConfig{
			Enabled:       tt.Timestamps.Enabled,
			CreatedColumn: tt.Timestamps.CreatedColumn,
			UpdatedColumn: tt.Timestamps.UpdatedColumn,
		}
	}

	table.Columns = make([]*core.Column, 0, len(tt.Columns))
	for i := range tt.Columns {
		col, err := convertColumn(&tt.Columns[i], dialect)
		if err != nil {
			return nil, fmt.Errorf("column %q: %w", tt.Columns[i].Name, err)
		}
		table.Columns = append(table.Columns, col)
	}

	if table.Timestamps != nil && table.Timestamps.Enabled {
		injectTimestampColumns(table)
	}

	table.Constraints = make([]*core.Constraint, 0, len(tt.Constraints))
	for i := range tt.Constraints {
		c := convertConstraint(&tt.Constraints[i])
		table.Constraints = append(table.Constraints, c)
	}

	if err := checkPKConflict(table); err != nil {
		return nil, err
	}

	synthesizeConstraints(table)

	table.Indexes = make([]*core.Index, 0, len(tt.Indexes))
	for i := range tt.Indexes {
		idx := convertIndex(&tt.Indexes[i])
		table.Indexes = append(table.Indexes, idx)
	}

	return table, nil
}

func convertTableOptions(to *tomlTableOptions) core.TableOptions {
	return core.TableOptions{
		Engine:       to.Engine,
		Charset:      to.Charset,
		Collate:      to.Collate,
		RowFormat:    to.RowFormat,
		Tablespace:   to.Tablespace,
		Compression:  to.Compression,
		Encryption:   to.Encryption,
		KeyBlockSize: to.KeyBlockSize,
	}
}

func convertColumn(tc *tomlColumn, dialect string) (*core.Column, error) {
	col := &core.Column{
		Name:          tc.Name,
		Nullable:      tc.Nullable,
		PrimaryKey:    tc.PrimaryKey,
		AutoIncrement: tc.AutoIncrement,
		Comment:       tc.Comment,
		Collate:       tc.Collate,
		Charset:       tc.Charset,
		Unique:        tc.Unique,
		Check:         tc.Check,
		References:    tc.References,
		EnumValues:    tc.EnumValues,
	}

	col.TypeRaw = resolveTypeRaw(tc)
	col.Type = core.NormalizeDataType(col.TypeRaw)

	if tc.RawType != "" && dialect != "" {
		col.RawType = tc.RawType
		col.RawTypeDialect = strings.ToLower(dialect)
	}

	if tc.DefaultValue != nil {
		s := normaliseDefault(tc.DefaultValue)
		col.DefaultValue = &s
	}
	if tc.References != "" {
		col.RefOnDelete = core.ReferentialAction(tc.OnDelete)
		col.RefOnUpdate = core.ReferentialAction(tc.OnUpdate)
	} else if tc.OnUpdate != "" {
		v := tc.OnUpdate
		col.OnUpdate = &v
	}

	col.IsGenerated = tc.IsGenerated
	col.GenerationExpression = tc.GenerationExpression
	if tc.GenerationStorage != "" {
		col.GenerationStorage = core.GenerationStorage(tc.GenerationStorage)
	}

	return col, nil
}

func resolveTypeRaw(tc *tomlColumn) string {
	t := strings.TrimSpace(tc.Type)

	if strings.EqualFold(t, "enum") && len(tc.EnumValues) > 0 {
		return core.BuildEnumTypeRaw(tc.EnumValues)
	}

	return t
}

func normaliseDefault(v any) string {
	switch val := v.(type) {
	case bool:
		if val {
			return "TRUE"
		}
		return "FALSE"
	case string:
		return val
	case int64:
		return strconv.FormatInt(val, 10)
	case float64:
		return strconv.FormatFloat(val, 'f', -1, 64)
	default:
		return fmt.Sprintf("%v", val)
	}
}

func convertConstraint(tc *tomlConstraint) *core.Constraint {
	c := &core.Constraint{
		Name:              tc.Name,
		Type:              core.ConstraintType(tc.Type),
		Columns:           tc.Columns,
		ReferencedTable:   tc.ReferencedTable,
		ReferencedColumns: tc.ReferencedColumns,
		OnDelete:          core.ReferentialAction(tc.OnDelete),
		OnUpdate:          core.ReferentialAction(tc.OnUpdate),
		CheckExpression:   tc.CheckExpression,
	}

	if tc.Enforced != nil {
		c.Enforced = *tc.Enforced
	} else {
		c.Enforced = true
	}

	return c
}

func checkPKConflict(table *core.Table) error {
	hasColumnPK := false
	for _, c := range table.Columns {
		if c.PrimaryKey {
			hasColumnPK = true
			break
		}
	}
	hasConstraintPK := false
	for _, c := range table.Constraints {
		if c.Type == core.ConstraintPrimaryKey {
			hasConstraintPK = true
			break
		}
	}
	if hasColumnPK && hasConstraintPK {
		return fmt.Errorf(
			"primary key declared on both column(s) and in constraints section; " +
				"use column-level primary_key for single-column PKs or a constraint for composite PKs, not both",
		)
	}
	return nil
}

func synthesizeConstraints(table *core.Table) {
	synthesizePK(table)
	synthesizeUniqueConstraints(table)
	synthesizeCheckConstraints(table)
	synthesizeFKConstraints(table)
}

func synthesizePK(table *core.Table) {
	for _, c := range table.Constraints {
		if c.Type == core.ConstraintPrimaryKey {
			return
		}
	}

	var pkCols []string
	for _, c := range table.Columns {
		if c.PrimaryKey {
			pkCols = append(pkCols, c.Name)
		}
	}
	if len(pkCols) == 0 {
		return
	}

	name := core.AutoGenerateConstraintName(core.ConstraintPrimaryKey, table.Name, pkCols, "")
	table.Constraints = append(table.Constraints, &core.Constraint{
		Name:    name,
		Type:    core.ConstraintPrimaryKey,
		Columns: pkCols,
	})
}

func synthesizeUniqueConstraints(table *core.Table) {
	for _, col := range table.Columns {
		if !col.Unique {
			continue
		}
		cols := []string{col.Name}
		name := core.AutoGenerateConstraintName(core.ConstraintUnique, table.Name, cols, "")
		table.Constraints = append(table.Constraints, &core.Constraint{
			Name:    name,
			Type:    core.ConstraintUnique,
			Columns: cols,
		})
	}
}

func synthesizeCheckConstraints(table *core.Table) {
	for _, col := range table.Columns {
		if col.Check == "" {
			continue
		}
		cols := []string{col.Name}
		name := core.AutoGenerateConstraintName(core.ConstraintCheck, table.Name, cols, "")
		table.Constraints = append(table.Constraints, &core.Constraint{
			Name:            name,
			Type:            core.ConstraintCheck,
			CheckExpression: col.Check,
			Enforced:        true,
		})
	}
}

func synthesizeFKConstraints(table *core.Table) {
	for _, col := range table.Columns {
		if col.References == "" {
			continue
		}
		refTable, refCol, ok := core.ParseReferences(col.References)
		if !ok {
			// Validation will catch this later.
			continue
		}
		cols := []string{col.Name}
		name := core.AutoGenerateConstraintName(core.ConstraintForeignKey, table.Name, cols, refTable)
		table.Constraints = append(table.Constraints, &core.Constraint{
			Name:              name,
			Type:              core.ConstraintForeignKey,
			Columns:           cols,
			ReferencedTable:   refTable,
			ReferencedColumns: []string{refCol},
			OnDelete:          col.RefOnDelete,
			OnUpdate:          col.RefOnUpdate,
			Enforced:          true,
		})
	}
}

func injectTimestampColumns(table *core.Table) {
	createdCol := "created_at"
	updatedCol := "updated_at"
	if table.Timestamps.CreatedColumn != "" {
		createdCol = table.Timestamps.CreatedColumn
	}
	if table.Timestamps.UpdatedColumn != "" {
		updatedCol = table.Timestamps.UpdatedColumn
	}

	if table.FindColumn(createdCol) == nil {
		def := "CURRENT_TIMESTAMP"
		table.Columns = append(table.Columns, &core.Column{
			Name:         createdCol,
			TypeRaw:      "timestamp",
			Type:         core.DataTypeDatetime,
			DefaultValue: &def,
		})
	}

	if table.FindColumn(updatedCol) == nil {
		def := "CURRENT_TIMESTAMP"
		upd := "CURRENT_TIMESTAMP"
		table.Columns = append(table.Columns, &core.Column{
			Name:         updatedCol,
			TypeRaw:      "timestamp",
			Type:         core.DataTypeDatetime,
			DefaultValue: &def,
			OnUpdate:     &upd,
		})
	}
}

func convertIndex(ti *tomlIndex) *core.Index {
	idx := &core.Index{
		Name:    ti.Name,
		Unique:  ti.Unique,
		Comment: ti.Comment,
	}

	if ti.Type != "" {
		idx.Type = core.IndexType(ti.Type)
	} else {
		idx.Type = core.IndexTypeBTree
	}

	if ti.Visibility != "" {
		idx.Visibility = core.IndexVisibility(ti.Visibility)
	} else {
		idx.Visibility = core.IndexVisible
	}

	idx.Columns = mergeIndexColumns(ti)

	return idx
}

func mergeIndexColumns(ti *tomlIndex) []core.IndexColumn {
	if len(ti.ColumnDefs) > 0 {
		cols := make([]core.IndexColumn, 0, len(ti.ColumnDefs))
		for i := range ti.ColumnDefs {
			cols = append(cols, convertIndexColumn(&ti.ColumnDefs[i]))
		}
		return cols
	}

	if len(ti.Columns) > 0 {
		cols := make([]core.IndexColumn, 0, len(ti.Columns))
		for _, name := range ti.Columns {
			cols = append(cols, core.IndexColumn{
				Name:  name,
				Order: core.SortAsc,
			})
		}
		return cols
	}

	return nil
}

func convertIndexColumn(tc *tomlIndexColumn) core.IndexColumn {
	ic := core.IndexColumn{
		Name:   tc.Name,
		Length: tc.Length,
	}

	if tc.Order != "" {
		ic.Order = core.SortOrder(tc.Order)
	} else {
		ic.Order = core.SortAsc
	}

	return ic
}
