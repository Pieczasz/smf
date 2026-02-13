package toml

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"smf/internal/core"
)

func (c *converter) convertColumn(tc *tomlColumn) (*core.Column, error) {
	if err := c.validateColumnName(tc.Name); err != nil {
		return nil, err
	}

	if tc.References != "" {
		if _, _, ok := core.ParseReferences(tc.References); !ok {
			return nil, fmt.Errorf("invalid references %q: expected format \"table.column\"", tc.References)
		}
	}

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

	if err := c.resolveColumnType(col, tc); err != nil {
		return nil, err
	}

	applyColumnActions(col, tc)

	return col, nil
}

func (c *converter) validateColumnName(name string) error {
	if strings.TrimSpace(name) == "" {
		return errors.New("column name is empty")
	}
	if c.rules != nil {
		if c.rules.MaxColumnNameLength > 0 && len(name) > c.rules.MaxColumnNameLength {
			return fmt.Errorf("column %q exceeds maximum length %d", name, c.rules.MaxColumnNameLength)
		}
		if c.nameRe != nil && !c.nameRe.MatchString(name) {
			return fmt.Errorf("column %q does not match allowed pattern %q", name, c.nameRe.String())
		}
	}
	return nil
}

// resolveColumnType populates col.Type and col.RawType from the TOML column,
// validating dialect-specific raw types when applicable.
func (c *converter) resolveColumnType(col *core.Column, tc *tomlColumn) error {
	portableType := strings.TrimSpace(tc.Type)

	if strings.EqualFold(portableType, "enum") && len(tc.EnumValues) > 0 {
		portableType = core.BuildEnumTypeRaw(tc.EnumValues)
	}

	if portableType == "" {
		return errors.New("type is empty")
	}

	col.Type = core.NormalizeDataType(portableType)

	if tc.RawType != "" && c.dialect != nil {
		if err := core.ValidateRawType(tc.RawType, c.dialect); err != nil {
			return fmt.Errorf("column %q: %w", tc.Name, err)
		}
		col.RawType = tc.RawType
	} else {
		col.RawType = portableType
	}

	return nil
}

// applyColumnActions sets default values, referential actions, on-update
// behavior, and generated-column properties on an already-initialized column.
func applyColumnActions(col *core.Column, tc *tomlColumn) {
	if tc.DefaultValue != nil {
		s := normalizeDefault(tc.DefaultValue)
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
}

func normalizeDefault(v any) string {
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
