package core

import (
	"fmt"
)

// validateEnums ensures that all fields using fixed string-based enums
// contain valid values.
func (db *Database) validateEnums() error {
	for _, table := range db.Tables {
		for _, col := range table.Columns {
			if err := col.validateEnums(table); err != nil {
				return err
			}
		}

		for _, con := range table.Constraints {
			if err := con.validateEnums(table); err != nil {
				return err
			}
		}

		for _, idx := range table.Indexes {
			if err := idx.validateEnums(table); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *Column) validateEnums(table *Table) error {
	if err := c.validateType(table); err != nil {
		return err
	}
	if err := c.validateRefActions(table); err != nil {
		return err
	}
	if err := c.validateGeneration(table); err != nil {
		return err
	}
	return c.validateIdentity(table)
}

func (c *Column) validateType(table *Table) error {
	if c.Type == "" {
		return nil
	}
	switch c.Type {
	case DataTypeString, DataTypeInt, DataTypeFloat, DataTypeBoolean,
		DataTypeDatetime, DataTypeJSON, DataTypeUUID, DataTypeBinary,
		DataTypeEnum, DataTypeUnknown:
		return nil
	default:
		return fmt.Errorf("table %q, column %q: invalid type %q", table.Name, c.Name, c.Type)
	}
}

func (c *Column) validateRefActions(table *Table) error {
	if c.RefOnDelete != "" && !isValidReferentialAction(c.RefOnDelete) {
		return fmt.Errorf("table %q, column %q: invalid ref_on_delete %q", table.Name, c.Name, c.RefOnDelete)
	}
	if c.RefOnUpdate != "" && !isValidReferentialAction(c.RefOnUpdate) {
		return fmt.Errorf("table %q, column %q: invalid ref_on_update %q", table.Name, c.Name, c.RefOnUpdate)
	}
	return nil
}

func (c *Column) validateGeneration(table *Table) error {
	if c.IsGenerated && c.GenerationStorage != "" {
		switch c.GenerationStorage {
		case GenerationVirtual, GenerationStored:
		default:
			return fmt.Errorf("table %q, column %q: invalid generation_storage %q", table.Name, c.Name, c.GenerationStorage)
		}
	}
	return nil
}

func (c *Column) validateIdentity(table *Table) error {
	if c.IdentityGeneration != "" {
		switch c.IdentityGeneration {
		case IdentityAlways, IdentityByDefault:
		default:
			return fmt.Errorf("table %q, column %q: invalid identity_generation %q", table.Name, c.Name, c.IdentityGeneration)
		}
	}
	return nil
}

func (con *Constraint) validateEnums(table *Table) error {
	switch con.Type {
	case ConstraintPrimaryKey, ConstraintForeignKey, ConstraintUnique, ConstraintCheck:
	default:
		return fmt.Errorf("table %q, constraint %q: invalid constraint type %q", table.Name, con.Name, con.Type)
	}

	if con.Type == ConstraintForeignKey {
		if con.OnDelete != "" {
			if !isValidReferentialAction(con.OnDelete) {
				return fmt.Errorf("table %q, constraint %q: invalid on_delete %q", table.Name, con.Name, con.OnDelete)
			}
		}
		if con.OnUpdate != "" {
			if !isValidReferentialAction(con.OnUpdate) {
				return fmt.Errorf("table %q, constraint %q: invalid on_update %q", table.Name, con.Name, con.OnUpdate)
			}
		}
	}

	return nil
}

func (i *Index) validateEnums(table *Table) error {
	if err := i.validateType(table); err != nil {
		return err
	}
	if err := i.validateVisibility(table); err != nil {
		return err
	}
	return i.validateColumnsOrder(table)
}

func (i *Index) validateType(table *Table) error {
	if i.Type == "" {
		return nil
	}
	switch i.Type {
	case IndexTypeBTree, IndexTypeHash, IndexTypeFullText, IndexTypeSpatial, IndexTypeGIN, IndexTypeGiST:
		return nil
	default:
		return fmt.Errorf("table %q, index %q: invalid index type %q", table.Name, i.Name, i.Type)
	}
}

func (i *Index) validateVisibility(table *Table) error {
	if i.Visibility == "" {
		return nil
	}
	switch i.Visibility {
	case IndexVisible, IndexInvisible:
		return nil
	default:
		return fmt.Errorf("table %q, index %q: invalid visibility %q", table.Name, i.Name, i.Visibility)
	}
}

func (i *Index) validateColumnsOrder(table *Table) error {
	for _, ic := range i.Columns {
		if ic.Order != "" {
			switch ic.Order {
			case SortAsc, SortDesc:
			default:
				return fmt.Errorf("table %q, index %q, column %q: invalid sort order %q", table.Name, i.Name, ic.Name, ic.Order)
			}
		}
	}
	return nil
}

func isValidReferentialAction(ra ReferentialAction) bool {
	switch ra {
	case RefActionNone, RefActionCascade, RefActionRestrict, RefActionSetNull, RefActionSetDefault, RefActionNoAction:
		return true
	default:
		return false
	}
}
