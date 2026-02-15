package core

import (
	"fmt"
	"strings"
)

// validateSemantic validates dialect-specific and logical rules that are not
// strictly structural.
func validateSemantic(db *Database, tableMap map[string]*Table) error {
	for _, table := range db.Tables {
		for _, col := range table.Columns {
			if err := validateColumnSemantic(table, col, db.Dialect); err != nil {
				return err
			}
		}

		if err := validateFKTypeMismatch(table, tableMap); err != nil {
			return err
		}
	}
	return nil
}

func validateColumnSemantic(table *Table, col *Column, dialect *Dialect) error {
	if err := validateColumnAutoIncrement(table, col, dialect); err != nil {
		return err
	}
	if err := validateColumnPrimaryKey(table, col); err != nil {
		return err
	}
	if err := validateColumnGenerationSemantic(table, col); err != nil {
		return err
	}
	if err := validateColumnIdentitySemantic(table, col); err != nil {
		return err
	}
	return validateDialectSpecificColumnSemantic(table, col, dialect)
}

func validateColumnAutoIncrement(table *Table, col *Column, dialect *Dialect) error {
	if col.AutoIncrement && col.Type != DataTypeInt {
		return fmt.Errorf("table %q, column %q: auto_increment is only allowed on integer columns", table.Name, col.Name)
	}
	if dialect != nil && *dialect == DialectSQLite && col.AutoIncrement && !col.PrimaryKey {
		return fmt.Errorf("table %q, column %q: SQLite AUTOINCREMENT is only allowed on PRIMARY KEY columns", table.Name, col.Name)
	}

	return nil
}

func validateColumnPrimaryKey(table *Table, col *Column) error {
	if (col.PrimaryKey || tableIsPartOfPK(table, col.Name)) && col.Nullable {
		return fmt.Errorf("table %q, column %q: primary key columns cannot be nullable", table.Name, col.Name)
	}
	return nil
}

func validateColumnGenerationSemantic(table *Table, col *Column) error {
	if col.IsGenerated && col.GenerationExpression == "" {
		return fmt.Errorf("table %q, column %q: generated column must have an expression", table.Name, col.Name)
	}
	return nil
}

func validateColumnIdentitySemantic(table *Table, col *Column) error {
	if (col.IdentitySeed != 0 || col.IdentityIncrement != 0) && !col.AutoIncrement {
		return fmt.Errorf("table %q, column %q: identity_seed and identity_increment can only be set for auto_increment columns", table.Name, col.Name)
	}
	return nil
}

func validateDialectSpecificColumnSemantic(table *Table, col *Column, dialect *Dialect) error {
	if dialect == nil {
		return nil
	}

	if *dialect == DialectTiDB {
		if col.TiDB != nil && col.TiDB.AutoRandom > 0 {
			if !col.PrimaryKey || col.Type != DataTypeInt {
				return fmt.Errorf("table %q, column %q: TiDB AUTO_RANDOM can only be applied to BIGINT PRIMARY KEY columns", table.Name, col.Name)
			}
		}
	}
	return nil
}

// validateFKTypeMismatch ensures that referencing and referenced columns in a
// Foreign Key have compatible types.
func validateFKTypeMismatch(table *Table, tableMap map[string]*Table) error {
	for _, con := range table.Constraints {
		if con.Type != ConstraintForeignKey {
			continue
		}

		refTable, ok := tableMap[strings.ToLower(con.ReferencedTable)]
		if !ok {
			continue
		}

		for i, colName := range con.Columns {
			if i >= len(con.ReferencedColumns) {
				continue
			}
			refColName := con.ReferencedColumns[i]

			col := table.FindColumn(colName)
			refCol := refTable.FindColumn(refColName)

			if col != nil && refCol != nil {
				if col.Type != refCol.Type {
					return fmt.Errorf("table %q, constraint %q: type mismatch between referencing column %q (%s) and referenced column %q (%s) in table %q",
						table.Name, con.Name, colName, col.Type, refColName, refCol.Type, con.ReferencedTable)
				}
			}
		}
	}
	return nil
}

// tableIsPartOfPK checks if a column name is included in any PRIMARY KEY constraint
// defined at the table level.
func tableIsPartOfPK(table *Table, colName string) bool {
	for _, con := range table.Constraints {
		if con.Type == ConstraintPrimaryKey {
			for _, c := range con.Columns {
				if strings.EqualFold(c, colName) {
					return true
				}
			}
		}
	}
	return false
}
