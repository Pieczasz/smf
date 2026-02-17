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

	// Invisible hides the column from SELECT * and some metadata views.
	Invisible bool `toml:"invisible"`

	// Identity / sequence fields for MSSQL, Oracle, DB2, PostgreSQL, Snowflake.
	IdentitySeed       int64  `toml:"identity_seed"`
	IdentityIncrement  int64  `toml:"identity_increment"`
	IdentityGeneration string `toml:"identity_generation"` // "ALWAYS" or "BY DEFAULT"
	SequenceName       string `toml:"sequence_name"`

	// Dialect-specific column option groups.
	MySQL  *tomlMySQLColumnOptions  `toml:"mysql"`
	TiDB   *tomlTiDBColumnOptions   `toml:"tidb"`
	Oracle *tomlOracleColumnOptions `toml:"oracle"`
	MSSQL  *tomlMSSQLColumnOptions  `toml:"mssql"`
	DB2    *tomlDB2ColumnOptions    `toml:"db2"`
	SQLite *tomlSQLiteColumnOptions `toml:"sqlite"`
}

// tomlMySQLColumnOptions maps [tables.columns.mysql].
type tomlMySQLColumnOptions struct {
	ColumnFormat             string `toml:"column_format"`
	Storage                  string `toml:"storage"`
	PrimaryEngineAttribute   string `toml:"primary_engine_attribute"`
	SecondaryEngineAttribute string `toml:"secondary_engine_attribute"`
}

// tomlTiDBColumnOptions maps [tables.columns.tidb].
type tomlTiDBColumnOptions struct {
	ShardBits uint64  `toml:"shard_bits"`
	RangeBits *uint64 `toml:"range_bits"`
}

// tomlOracleColumnOptions maps [tables.columns.oracle].
type tomlOracleColumnOptions struct {
	Encrypt             bool   `toml:"encrypt"`
	EncryptionAlgorithm string `toml:"encryption_algorithm"`
	Salt                *bool  `toml:"salt"`
	DefaultOnNull       bool   `toml:"default_on_null"`
}

// tomlMSSQLColumnOptions maps [tables.columns.mssql].
type tomlMSSQLColumnOptions struct {
	FileStream                bool                      `toml:"file_stream"`
	Sparse                    bool                      `toml:"sparse"`
	RowGUIDCol                bool                      `toml:"row_guid_col"`
	IdentityNotForReplication bool                      `toml:"identity_not_for_replication"`
	Persisted                 bool                      `toml:"persisted"`
	AlwaysEncrypted           *tomlMSSQLAlwaysEncrypted `toml:"always_encrypted"`
	DataMasking               *tomlMSSQLDataMasking     `toml:"data_masking"`
}

type tomlMSSQLAlwaysEncrypted struct {
	ColumnEncryptionKey string `toml:"column_encryption_key"`
	EncryptionType      string `toml:"encryption_type"`
	Algorithm           string `toml:"algorithm"`
}

type tomlMSSQLDataMasking struct {
	Function string `toml:"function"`
}

// tomlDB2ColumnOptions maps [tables.columns.db2].
type tomlDB2ColumnOptions struct {
	InlineLength     *int  `toml:"inline_length"`
	Compress         *bool `toml:"compress"`
	ImplicitlyHidden bool  `toml:"implicitly_hidden"`
}

// tomlSQLiteColumnOptions maps [tables.columns.sqlite].
type tomlSQLiteColumnOptions struct {
	StrictAutoincrement bool `toml:"strict_autoincrement"`
}

func (p *Parser) parseColumn(tc *tomlColumn) (*core.Column, error) {
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
		Invisible:          tc.Invisible,
	}

	if err := p.resolveColumnType(col, tc); err != nil {
		return nil, err
	}

	applyColumnActions(col, tc)
	applyColumnDialectOptions(col, tc)

	return col, nil
}

// resolveColumnType populates col.Type and col.RawType from the TOML column.
func (p *Parser) resolveColumnType(col *core.Column, tc *tomlColumn) error {
	portableType := strings.TrimSpace(tc.Type)

	if strings.EqualFold(portableType, "enum") && len(tc.EnumValues) > 0 {
		portableType = core.BuildEnumTypeRaw(tc.EnumValues)
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
			PrimaryEngineAttribute:   tc.MySQL.PrimaryEngineAttribute,
			SecondaryEngineAttribute: tc.MySQL.SecondaryEngineAttribute,
		}
	}
	if tc.TiDB != nil {
		col.TiDB = &core.TiDBColumnOptions{
			ShardBits: tc.TiDB.ShardBits,
			RangeBits: tc.TiDB.RangeBits,
		}
	}
	if tc.Oracle != nil {
		col.Oracle = &core.OracleColumnOptions{
			Encrypt:             tc.Oracle.Encrypt,
			EncryptionAlgorithm: tc.Oracle.EncryptionAlgorithm,
			Salt:                tc.Oracle.Salt,
			DefaultOnNull:       tc.Oracle.DefaultOnNull,
		}
	}
	if tc.MSSQL != nil {
		col.MSSQL = &core.MSSQLColumnOptions{
			FileStream:                tc.MSSQL.FileStream,
			Sparse:                    tc.MSSQL.Sparse,
			RowGUIDCol:                tc.MSSQL.RowGUIDCol,
			IdentityNotForReplication: tc.MSSQL.IdentityNotForReplication,
			Persisted:                 tc.MSSQL.Persisted,
		}
		if tc.MSSQL.AlwaysEncrypted != nil {
			col.MSSQL.AlwaysEncrypted = &core.MSSQLAlwaysEncryptedOptions{
				ColumnEncryptionKey: tc.MSSQL.AlwaysEncrypted.ColumnEncryptionKey,
				EncryptionType:      tc.MSSQL.AlwaysEncrypted.EncryptionType,
				Algorithm:           tc.MSSQL.AlwaysEncrypted.Algorithm,
			}
		}
		if tc.MSSQL.DataMasking != nil {
			col.MSSQL.DataMasking = &core.MSSQLDataMaskingOptions{
				Function: tc.MSSQL.DataMasking.Function,
			}
		}
	}
	if tc.DB2 != nil {
		col.DB2 = &core.DB2ColumnOptions{
			InlineLength:     tc.DB2.InlineLength,
			Compress:         tc.DB2.Compress,
			ImplicitlyHidden: tc.DB2.ImplicitlyHidden,
		}
	}
	if tc.SQLite != nil {
		col.SQLite = &core.SQLiteColumnOptions{
			StrictAutoincrement: tc.SQLite.StrictAutoincrement,
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
