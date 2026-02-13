package toml

import (
	"errors"
	"fmt"
	"strings"

	"smf/internal/core"
)

func (c *converter) convertTable(tt *tomlTable) (*core.Table, error) {
	if err := c.validateTableName(tt.Name); err != nil {
		return nil, err
	}

	table := &core.Table{
		Name:    tt.Name,
		Comment: tt.Comment,
		Options: convertTableOptions(&tt.Options),
	}

	if ts := tt.Timestamps; ts != nil {
		table.Timestamps = &core.TimestampsConfig{
			Enabled:       ts.Enabled,
			CreatedColumn: ts.CreatedColumn,
			UpdatedColumn: ts.UpdatedColumn,
		}
	}

	if err := c.convertTableColumns(table, tt); err != nil {
		return nil, err
	}

	table.Constraints = make([]*core.Constraint, 0, len(tt.Constraints))
	for i := range tt.Constraints {
		con := convertTableConstraint(&tt.Constraints[i])
		table.Constraints = append(table.Constraints, con)
	}

	if err := checkPKConflict(table); err != nil {
		return nil, err
	}

	synthesizeConstraints(table)

	table.Indexes = make([]*core.Index, 0, len(tt.Indexes))
	for i := range tt.Indexes {
		idx, err := convertTableIndex(&tt.Indexes[i])
		if err != nil {
			return nil, fmt.Errorf("index %q: %w", tt.Indexes[i].Name, err)
		}
		table.Indexes = append(table.Indexes, idx)
	}

	if err := validateConstraints(table); err != nil {
		return nil, err
	}
	if err := validateIndexes(table); err != nil {
		return nil, err
	}

	return table, nil
}

// validateTableName checks emptiness, duplicates, length, and pattern - all
// before we spend any time converting columns.
func (c *converter) validateTableName(name string) error {
	if strings.TrimSpace(name) == "" {
		return fmt.Errorf("table name is empty")
	}

	lower := strings.ToLower(name)
	if c.seenTables[lower] {
		return fmt.Errorf("duplicate table name %q", name)
	}
	c.seenTables[lower] = true

	if c.rules != nil {
		if c.rules.MaxTableNameLength > 0 && len(name) > c.rules.MaxTableNameLength {
			return fmt.Errorf("table %q exceeds maximum length %d", name, c.rules.MaxTableNameLength)
		}
		if c.nameRe != nil && !c.nameRe.MatchString(name) {
			return fmt.Errorf("table %q does not match allowed pattern %q", name, c.nameRe.String())
		}
	}

	return nil
}

func convertTableOptions(to *tomlTableOptions) core.TableOptions {
	opts := core.TableOptions{
		Tablespace: to.Tablespace,
	}

	// TODO: handle dialect specific options instead of mysql only.
	// Route MySQL-family options into the dialect-specific sub-struct.
	if to.Engine != "" || to.Charset != "" || to.Collate != "" ||
		to.RowFormat != "" || to.Compression != "" ||
		to.Encryption != "" || to.KeyBlockSize != 0 {
		opts.MySQL = &core.MySQLTableOptions{
			Engine:       to.Engine,
			Charset:      to.Charset,
			Collate:      to.Collate,
			RowFormat:    to.RowFormat,
			Compression:  to.Compression,
			Encryption:   to.Encryption,
			KeyBlockSize: to.KeyBlockSize,
		}
	}

	return opts
}

// convertTableColumns populates table.Columns from the TOML column definitions,
// injects timestamp columns when enabled, and ensures the table is non-empty.
func (c *converter) convertTableColumns(table *core.Table, tt *tomlTable) error {
	table.Columns = make([]*core.Column, 0, len(tt.Columns))
	seenCols := make(map[string]bool, len(tt.Columns))
	for i := range tt.Columns {
		col, err := c.convertColumn(&tt.Columns[i])
		if err != nil {
			return fmt.Errorf("column %q: %w", tt.Columns[i].Name, err)
		}
		lower := strings.ToLower(col.Name)
		if seenCols[lower] {
			return fmt.Errorf("duplicate column name %q", col.Name)
		}
		seenCols[lower] = true
		table.Columns = append(table.Columns, col)
	}

	if table.Timestamps != nil && table.Timestamps.Enabled {
		if err := injectTimestampColumns(table); err != nil {
			return err
		}
	}

	if len(table.Columns) == 0 {
		return errors.New("table has no columns")
	}
	return nil
}

// injectTimestampColumns resolves the created/updated column names, validates
// they are distinct, and appends the columns when not already present.
func injectTimestampColumns(table *core.Table) error {
	createdCol := "created_at"
	updatedCol := "updated_at"
	if table.Timestamps.CreatedColumn != "" {
		createdCol = table.Timestamps.CreatedColumn
	}
	if table.Timestamps.UpdatedColumn != "" {
		updatedCol = table.Timestamps.UpdatedColumn
	}

	if strings.EqualFold(createdCol, updatedCol) {
		return fmt.Errorf("timestamps created_column and updated_column resolve to the same name %q", createdCol)
	}

	if table.FindColumn(createdCol) == nil {
		def := "CURRENT_TIMESTAMP"
		table.Columns = append(table.Columns, &core.Column{
			Name:         createdCol,
			RawType:      "timestamp",
			Type:         core.DataTypeDatetime,
			DefaultValue: &def,
		})
	}

	if table.FindColumn(updatedCol) == nil {
		def := "CURRENT_TIMESTAMP"
		upd := "CURRENT_TIMESTAMP"
		table.Columns = append(table.Columns, &core.Column{
			Name:         updatedCol,
			RawType:      "timestamp",
			Type:         core.DataTypeDatetime,
			DefaultValue: &def,
			OnUpdate:     &upd,
		})
	}

	return nil
}
