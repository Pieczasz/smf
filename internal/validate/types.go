package validate

import "smf/internal/schema"

// IdentityGeneration reports whether ig is a recognized identity generation mode.
func IdentityGeneration(ig schema.IdentityGeneration) bool {
	switch ig {
	case schema.IdentityAlways, schema.IdentityByDefault:
		return true
	default:
		return false
	}
}

// DataType reports whether d is a recognized portable data type.
func DataType(d schema.DataType) bool {
	switch d {
	case schema.DataTypeString, schema.DataTypeInt, schema.DataTypeFloat,
		schema.DataTypeBoolean, schema.DataTypeDatetime, schema.DataTypeJSON,
		schema.DataTypeUUID, schema.DataTypeBinary, schema.DataTypeEnum,
		schema.DataTypeUnknown:
		return true
	default:
		return false
	}
}

// GenerationStorage reports whether gs is a recognized generation storage mode.
func GenerationStorage(gs schema.GenerationStorage) bool {
	switch gs {
	case schema.GenerationVirtual, schema.GenerationStored:
		return true
	default:
		return false
	}
}

// ConstraintType reports whether ct is a recognized constraint type.
func ConstraintType(ct schema.ConstraintType) bool {
	switch ct {
	case schema.ConstraintPrimaryKey, schema.ConstraintForeignKey, schema.ConstraintUnique, schema.ConstraintCheck:
		return true
	default:
		return false
	}
}

// ReferentialAction reports whether ra is a recognized referential action (including empty/none).
func ReferentialAction(ra schema.ReferentialAction) bool {
	switch ra {
	case schema.RefActionNone, schema.RefActionCascade, schema.RefActionRestrict, schema.RefActionSetNull, schema.RefActionSetDefault, schema.RefActionNoAction:
		return true
	default:
		return false
	}
}

// IndexType reports whether it is a recognized index type.
func IndexType(it schema.IndexType) bool {
	switch it {
	case schema.IndexTypeBTree, schema.IndexTypeHash, schema.IndexTypeFullText, schema.IndexTypeSpatial, schema.IndexTypeGIN, schema.IndexTypeGiST:
		return true
	default:
		return false
	}
}

// IndexVisibility reports whether iv is a recognized index visibility.
func IndexVisibility(iv schema.IndexVisibility) bool {
	switch iv {
	case schema.IndexVisible, schema.IndexInvisible:
		return true
	default:
		return false
	}
}

// SortOrder reports whether so is a recognized sort order.
func SortOrder(so schema.SortOrder) bool {
	switch so {
	case schema.SortAsc, schema.SortDesc:
		return true
	default:
		return false
	}
}
