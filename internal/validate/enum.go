package validate

import (
	"fmt"

	"smf/internal/schema"
)

func Enums(db *schema.Database) error {
	for _, table := range db.Tables {
		for _, col := range table.Columns {
			if err := ColumnEnums(col, table); err != nil {
				return err
			}
		}

		for _, con := range table.Constraints {
			if err := ConstraintEnums(con, table); err != nil {
				return err
			}
		}

		for _, idx := range table.Indexes {
			if err := IndexEnums(idx, table); err != nil {
				return err
			}
		}
	}
	return nil
}

func ColumnEnums(c *schema.Column, table *schema.Table) error {
	if err := ColumnType(c, table); err != nil {
		return err
	}
	if err := RefActions(c, table); err != nil {
		return err
	}
	if err := Generation(c, table); err != nil {
		return err
	}
	return Identity(c, table)
}

func ColumnType(c *schema.Column, table *schema.Table) error {
	if c.Type == "" {
		return nil
	}
	if !DataType(c.Type) {
		return fmt.Errorf("table %q, column %q: invalid type %q", table.Name, c.Name, c.Type)
	}
	return nil
}

func RefActions(c *schema.Column, table *schema.Table) error {
	if c.RefOnDelete != "" && !ReferentialAction(c.RefOnDelete) {
		return fmt.Errorf("table %q, column %q: invalid ref_on_delete %q", table.Name, c.Name, c.RefOnDelete)
	}
	if c.RefOnUpdate != "" && !ReferentialAction(c.RefOnUpdate) {
		return fmt.Errorf("table %q, column %q: invalid ref_on_update %q", table.Name, c.Name, c.RefOnUpdate)
	}
	return nil
}

func Generation(c *schema.Column, table *schema.Table) error {
	if c.IsGenerated && c.GenerationStorage != "" && !GenerationStorage(c.GenerationStorage) {
		return fmt.Errorf("table %q, column %q: invalid generation_storage %q", table.Name, c.Name, c.GenerationStorage)
	}
	return nil
}

func Identity(c *schema.Column, table *schema.Table) error {
	if c.IdentityGeneration != "" && !IdentityGeneration(c.IdentityGeneration) {
		return fmt.Errorf("table %q, column %q: invalid identity_generation %q", table.Name, c.Name, c.IdentityGeneration)
	}
	return nil
}

func ConstraintEnums(con *schema.Constraint, table *schema.Table) error {
	if !ConstraintType(con.Type) {
		return fmt.Errorf("table %q, constraint %q: invalid constraint type %q", table.Name, con.Name, con.Type)
	}

	if con.Type == schema.ConstraintForeignKey {
		if con.OnDelete != "" && !ReferentialAction(con.OnDelete) {
			return fmt.Errorf("table %q, constraint %q: invalid on_delete %q", table.Name, con.Name, con.OnDelete)
		}
		if con.OnUpdate != "" && !ReferentialAction(con.OnUpdate) {
			return fmt.Errorf("table %q, constraint %q: invalid on_update %q", table.Name, con.Name, con.OnUpdate)
		}
	}

	return nil
}

func IndexEnums(i *schema.Index, table *schema.Table) error {
	if err := checkIndexType(i, table); err != nil {
		return err
	}
	if err := checkIndexVisibility(i, table); err != nil {
		return err
	}
	return IndexColumnsOrder(i, table)
}

func checkIndexType(i *schema.Index, table *schema.Table) error {
	if i.Type == "" {
		return nil
	}
	if !IndexType(i.Type) {
		return fmt.Errorf("table %q, index %q: invalid index type %q", table.Name, i.Name, i.Type)
	}
	return nil
}

func checkIndexVisibility(i *schema.Index, table *schema.Table) error {
	if i.Visibility == "" {
		return nil
	}
	if !IndexVisibility(i.Visibility) {
		return fmt.Errorf("table %q, index %q: invalid visibility %q", table.Name, i.Name, i.Visibility)
	}
	return nil
}

func IndexColumnsOrder(i *schema.Index, table *schema.Table) error {
	for _, ic := range i.Columns {
		if ic.Order != "" && !SortOrder(ic.Order) {
			return fmt.Errorf("table %q, index %q, column %q: invalid sort order %q", table.Name, i.Name, ic.Name, ic.Order)
		}
	}
	return nil
}
