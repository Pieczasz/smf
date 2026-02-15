package toml

import (
	"fmt"
	"strconv"
	"strings"

	"smf/internal/core"
)

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
	// The converter normalizes everything to a string.
	// In the new schema this is the `default` key (was `default_value`).
	DefaultValue any `toml:"default"`

	// OnUpdate is used for MySQL ON UPDATE CURRENT_TIMESTAMP when there is
	// no inline FK (references are empty).  When references ARE set,
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

	// Identity / sequence fields for MSSQL, Oracle, DB2, PostgreSQL, Snowflake.
	IdentitySeed       int64  `toml:"identity_seed"`
	IdentityIncrement  int64  `toml:"identity_increment"`
	IdentityGeneration string `toml:"identity_generation"` // "ALWAYS" or "BY DEFAULT"
	SequenceName       string `toml:"sequence_name"`

	// Dialect-specific column option groups.
	MySQL *tomlMySQLColumnOptions `toml:"mysql"`
	TiDB  *tomlTiDBColumnOptions  `toml:"tidb"`
}

// tomlMySQLColumnOptions maps [tables.columns.mysql].
type tomlMySQLColumnOptions struct {
	ColumnFormat             string `toml:"column_format"`
	Storage                  string `toml:"storage"`
	SecondaryEngineAttribute string `toml:"secondary_engine_attribute"`
}

// tomlTiDBColumnOptions maps [tables.columns.tidb].
type tomlTiDBColumnOptions struct {
	AutoRandom uint64 `toml:"auto_random"`
}

func (c *converter) convertColumn(tc *tomlColumn) (*core.Column, error) {
	col := &core.Column{
		Name:               tc.Name,
		Nullable:           tc.Nullable,
		PrimaryKey:         tc.PrimaryKey,
		AutoIncrement:      tc.AutoIncrement,
		Comment:            tc.Comment,
		Collate:            tc.Collate,
		Charset:            tc.Charset,
		Unique:             tc.Unique,
		Check:              tc.Check,
		References:         tc.References,
		EnumValues:         tc.EnumValues,
		IdentitySeed:       tc.IdentitySeed,
		IdentityIncrement:  tc.IdentityIncrement,
		IdentityGeneration: core.IdentityGeneration(tc.IdentityGeneration),
		SequenceName:       tc.SequenceName,
	}

	if err := c.resolveColumnType(col, tc); err != nil {
		return nil, err
	}

	applyColumnActions(col, tc)
	applyColumnDialectOptions(col, tc)

	return col, nil
}

// resolveColumnType populates col.Type and col.RawType from the TOML column.
func (c *converter) resolveColumnType(col *core.Column, tc *tomlColumn) error {
	portableType := strings.TrimSpace(tc.Type)

	if strings.EqualFold(portableType, "enum") && len(tc.EnumValues) > 0 {
		portableType = core.BuildEnumTypeRaw(tc.EnumValues)
	}

	if portableType == "" {
		return fmt.Errorf("column %q: type is empty", tc.Name)
	}

	col.Type = core.NormalizeDataType(portableType)

	if tc.RawType != "" {
		col.RawType = tc.RawType
	}

	return nil
}

// applyColumnActions sets default values, referential actions, on-update
// behavior, and generated-column properties on an already-initialized column.
func applyColumnActions(col *core.Column, tc *tomlColumn) {
	if tc.DefaultValue != nil {
		col.DefaultValue = new(normalizeDefault(tc.DefaultValue))
	}
	if tc.References != "" {
		col.RefOnDelete = core.ReferentialAction(tc.OnDelete)
		col.RefOnUpdate = core.ReferentialAction(tc.OnUpdate)
	} else if tc.OnUpdate != "" {
		col.OnUpdate = new(tc.OnUpdate)
	}

	col.IsGenerated = tc.IsGenerated
	col.GenerationExpression = tc.GenerationExpression
	if tc.GenerationStorage != "" {
		col.GenerationStorage = core.GenerationStorage(tc.GenerationStorage)
	}
}

// applyColumnDialectOptions converts dialect-specific column option groups
// from the TOML representation to the core model.
func applyColumnDialectOptions(col *core.Column, tc *tomlColumn) {
	if tc.MySQL != nil {
		col.MySQL = &core.MySQLColumnOptions{
			ColumnFormat:             tc.MySQL.ColumnFormat,
			Storage:                  tc.MySQL.Storage,
			SecondaryEngineAttribute: tc.MySQL.SecondaryEngineAttribute,
		}
	}
	if tc.TiDB != nil {
		col.TiDB = &core.TiDBColumnOptions{
			AutoRandom: tc.TiDB.AutoRandom,
		}
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
