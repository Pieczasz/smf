package core

import (
	"fmt"
)

// validateEnums ensures that all fields using fixed string-based enums
// contain valid values.
func validateEnums(db *Database) error {
	for _, table := range db.Tables {
		for _, col := range table.Columns {
			if err := validateColumnEnums(table, col); err != nil {
				return err
			}
		}

		for _, con := range table.Constraints {
			if err := validateConstraintEnums(table, con); err != nil {
				return err
			}
		}

		for _, idx := range table.Indexes {
			if err := validateIndexEnums(table, idx); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateColumnEnums(table *Table, col *Column) error {
	if err := validateColumnType(table, col); err != nil {
		return err
	}
	if err := validateColumnRefActions(table, col); err != nil {
		return err
	}
	if err := validateColumnGeneration(table, col); err != nil {
		return err
	}
	return validateColumnIdentity(table, col)
}

func validateColumnType(table *Table, col *Column) error {
	if col.Type == "" {
		return nil
	}
	switch col.Type {
	case DataTypeString, DataTypeInt, DataTypeFloat, DataTypeBoolean,
		DataTypeDatetime, DataTypeJSON, DataTypeUUID, DataTypeBinary,
		DataTypeEnum, DataTypeUnknown:
		return nil
	default:
		return fmt.Errorf("table %q, column %q: invalid type %q", table.Name, col.Name, col.Type)
	}
}

func validateColumnRefActions(table *Table, col *Column) error {
	if col.RefOnDelete != "" && !isValidReferentialAction(col.RefOnDelete) {
		return fmt.Errorf("table %q, column %q: invalid ref_on_delete %q", table.Name, col.Name, col.RefOnDelete)
	}
	if col.RefOnUpdate != "" && !isValidReferentialAction(col.RefOnUpdate) {
		return fmt.Errorf("table %q, column %q: invalid ref_on_update %q", table.Name, col.Name, col.RefOnUpdate)
	}
	return nil
}

func validateColumnGeneration(table *Table, col *Column) error {
	if col.IsGenerated && col.GenerationStorage != "" {
		switch col.GenerationStorage {
		case GenerationVirtual, GenerationStored:
		default:
			return fmt.Errorf("table %q, column %q: invalid generation_storage %q", table.Name, col.Name, col.GenerationStorage)
		}
	}
	return nil
}

func validateColumnIdentity(table *Table, col *Column) error {
	if col.IdentityGeneration != "" {
		switch col.IdentityGeneration {
		case IdentityAlways, IdentityByDefault:
		default:
			return fmt.Errorf("table %q, column %q: invalid identity_generation %q", table.Name, col.Name, col.IdentityGeneration)
		}
	}
	return nil
}

func validateConstraintEnums(table *Table, con *Constraint) error {
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

func validateIndexEnums(table *Table, idx *Index) error {
	if err := validateIndexType(table, idx); err != nil {
		return err
	}
	if err := validateIndexVisibility(table, idx); err != nil {
		return err
	}
	return validateIndexColumnsOrder(table, idx)
}

func validateIndexType(table *Table, idx *Index) error {
	if idx.Type == "" {
		return nil
	}
	switch idx.Type {
	case IndexTypeBTree, IndexTypeHash, IndexTypeFullText, IndexTypeSpatial, IndexTypeGIN, IndexTypeGiST:
		return nil
	default:
		return fmt.Errorf("table %q, index %q: invalid index type %q", table.Name, idx.Name, idx.Type)
	}
}

func validateIndexVisibility(table *Table, idx *Index) error {
	if idx.Visibility == "" {
		return nil
	}
	switch idx.Visibility {
	case IndexVisible, IndexInvisible:
		return nil
	default:
		return fmt.Errorf("table %q, index %q: invalid visibility %q", table.Name, idx.Name, idx.Visibility)
	}
}

func validateIndexColumnsOrder(table *Table, idx *Index) error {
	for _, ic := range idx.Columns {
		if ic.Order != "" {
			switch ic.Order {
			case SortAsc, SortDesc:
			default:
				return fmt.Errorf("table %q, index %q, column %q: invalid sort order %q", table.Name, idx.Name, ic.Name, ic.Order)
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
