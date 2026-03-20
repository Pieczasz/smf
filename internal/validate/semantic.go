package validate

import (
	"fmt"
	"slices"

	"smf/internal/schema"
)

func LogicalRules(tables []*schema.Table, dialect schema.Dialect) error {
	for _, table := range tables {
		for _, col := range table.Columns {
			if err := ColumnLogicalRules(col, table, dialect); err != nil {
				return err
			}
		}
		if err := ForeignKeyTypeCompatibility(table, tables); err != nil {
			return err
		}
	}
	return nil
}

func ColumnLogicalRules(c *schema.Column, table *schema.Table, dialect schema.Dialect) error {
	if c.RawType != "" {
		if err := schema.ValidateRawType(c.RawType, dialect); err != nil {
			return fmt.Errorf("table %q, column %q: %w", table.Name, c.Name, err)
		}
	}
	if err := AutoIncrement(c, table, dialect); err != nil {
		return err
	}
	if err := PrimaryKeyNullable(c, table); err != nil {
		return err
	}
	if err := GenerationSemantic(c, table); err != nil {
		return err
	}
	if err := IdentitySemantic(c, table); err != nil {
		return err
	}
	return DialectSpecificSemantic(c, table, dialect)
}

func AutoIncrement(c *schema.Column, table *schema.Table, dialect schema.Dialect) error {
	if c.AutoIncrement && c.Type != schema.DataTypeInt {
		return fmt.Errorf("table %q, column %q: auto_increment is only allowed on integer columns", table.Name, c.Name)
	}
	if dialect == schema.DialectSQLite && c.AutoIncrement && !c.PrimaryKey {
		return fmt.Errorf("table %q, column %q: SQLite AUTOINCREMENT is only allowed on PRIMARY KEY columns", table.Name, c.Name)
	}

	return nil
}

func PrimaryKeyNullable(c *schema.Column, table *schema.Table) error {
	if (c.PrimaryKey || PartOfPrimaryKey(table, c.Name)) && c.Nullable {
		return fmt.Errorf("table %q, column %q: primary key columns cannot be nullable", table.Name, c.Name)
	}
	return nil
}

func GenerationSemantic(c *schema.Column, table *schema.Table) error {
	if c.IsGenerated && c.GenerationExpression == "" {
		return fmt.Errorf("table %q, column %q: generated column must have an expression", table.Name, c.Name)
	}
	return nil
}

func IdentitySemantic(c *schema.Column, table *schema.Table) error {
	if (c.IdentitySeed != 0 || c.IdentityIncrement != 0) && !c.AutoIncrement {
		return fmt.Errorf("table %q, column %q: identity_seed and identity_increment can only be set for auto_increment columns", table.Name, c.Name)
	}
	return nil
}

func DialectSpecificSemantic(c *schema.Column, table *schema.Table, dialect schema.Dialect) error {
	if dialect == schema.DialectTiDB {
		if c.TiDB != nil && c.TiDB.ShardBits > 0 {
			if !c.PrimaryKey || c.Type != schema.DataTypeInt {
				return fmt.Errorf("table %q, column %q: TiDB AUTO_RANDOM can only be applied to BIGINT PRIMARY KEY columns", table.Name, c.Name)
			}
		}
	}
	return nil
}

func ForeignKeyTypeCompatibility(t *schema.Table, tables []*schema.Table) error {
	for _, con := range t.Constraints {
		if con.Type != schema.ConstraintForeignKey {
			continue
		}
		refTable := FindTable(tables, con.ReferencedTable)
		if refTable == nil {
			continue
		}
		for i, colName := range con.Columns {
			if i >= len(con.ReferencedColumns) {
				continue
			}
			refColName := con.ReferencedColumns[i]
			col := t.FindColumn(colName)
			refCol := refTable.FindColumn(refColName)
			if col != nil && refCol != nil && col.Type != refCol.Type {
				return fmt.Errorf("table %q, constraint %q: type mismatch between referencing column %q (%s) and referenced column %q (%s) in table %q",
					t.Name, con.Name, colName, col.Type, refColName, refCol.Type, con.ReferencedTable)
			}
		}
	}
	return nil
}

func PartOfPrimaryKey(t *schema.Table, colName string) bool {
	for _, con := range t.Constraints {
		if con.Type == schema.ConstraintPrimaryKey {
			if slices.Contains(con.Columns, colName) {
				return true
			}
		}
	}
	return false
}
