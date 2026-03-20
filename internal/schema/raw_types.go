package schema

import (
	"errors"
	"fmt"
	"maps"
	"regexp"
	"sort"
	"strings"
)

// parenRe matches balanced parentheses and their content so we can
// extract the base type name. Example: "VARCHAR(255)" -> "VARCHAR".
var parenRe = regexp.MustCompile(`\([^)]*\)`)

// wsRe collapses runs of whitespace into a single space after the
// parenthesized parts have been removed.
var wsRe = regexp.MustCompile(`\s+`)

// modifierRe patterns for stripping SQL type modifiers.
var (
	modifierUnsignedRe = regexp.MustCompile(`(?i)\bUNSIGNED\b`)
	modifierSignedRe   = regexp.MustCompile(`(?i)\bSIGNED\b`)
	modifierZerofillRe = regexp.MustCompile(`(?i)\bZEROFILL\b`)
)

// dialectRawTypes maps each supported dialect to its set of valid base
// type keywords (upper-cased).
var dialectRawTypes = map[Dialect]map[string]struct{}{
	DialectMySQL:      mysqlTypes,
	DialectMariaDB:    mariadbTypes,
	DialectPostgreSQL: postgresqlTypes,
	DialectSQLite:     sqliteTypes,
	DialectOracle:     oracleTypes,
	DialectDB2:        db2Types,
	DialectSnowflake:  snowflakeTypes,
	DialectMSSQL:      mssqlTypes,
	DialectTiDB:       tidbTypes,
}

var mysqlBaseTypes = toSet(
	// Numeric
	"TINYINT", "SMALLINT", "MEDIUMINT", "INT", "INTEGER", "BIGINT",
	"FLOAT", "DOUBLE", "DOUBLE PRECISION", "DECIMAL", "DEC", "NUMERIC",
	"FIXED", "BIT", "BOOL", "BOOLEAN",

	// Date / Time
	"DATE", "DATETIME", "TIMESTAMP", "TIME", "YEAR",

	// String
	"CHAR", "VARCHAR", "BINARY", "VARBINARY",
	"TINYBLOB", "BLOB", "MEDIUMBLOB", "LONGBLOB",
	"TINYTEXT", "TEXT", "MEDIUMTEXT", "LONGTEXT",

	// Special
	"ENUM", "SET", "JSON",

	// Spatial
	"GEOMETRY", "POINT", "LINESTRING", "POLYGON",
	"MULTIPOINT", "MULTILINESTRING", "MULTIPOLYGON", "GEOMETRYCOLLECTION",
)

var mysqlTypes = mysqlBaseTypes

var mariadbTypes = mergeSets(mysqlBaseTypes, toSet("INET4", "INET6", "UUID"))

var tidbTypes = mysqlBaseTypes

var postgresqlTypes = toSet(
	// Numeric
	"SMALLINT", "INT2", "INTEGER", "INT", "INT4", "BIGINT", "INT8",
	"DECIMAL", "NUMERIC", "REAL", "FLOAT4",
	"DOUBLE PRECISION", "FLOAT8", "FLOAT",
	"SMALLSERIAL", "SERIAL2", "SERIAL", "SERIAL4", "BIGSERIAL", "SERIAL8",
	"MONEY",

	// Character
	"CHARACTER", "CHAR", "CHARACTER VARYING", "VARCHAR", "TEXT",

	// Binary
	"BYTEA",

	// Date / Time
	"TIMESTAMP", "TIMESTAMP WITHOUT TIME ZONE", "TIMESTAMP WITH TIME ZONE", "TIMESTAMPTZ",
	"DATE",
	"TIME", "TIME WITHOUT TIME ZONE", "TIME WITH TIME ZONE", "TIMETZ",
	"INTERVAL",

	// Boolean
	"BOOLEAN", "BOOL",

	// Enum (user-defined, but keyword is valid)
	"ENUM",

	// Geometric
	"POINT", "LINE", "LSEG", "BOX", "PATH", "POLYGON", "CIRCLE",

	// Network
	"CIDR", "INET", "MACADDR", "MACADDR8",

	// Bit string
	"BIT", "BIT VARYING", "VARBIT",

	// Text search
	"TSVECTOR", "TSQUERY", "GTSVECTOR",

	// UUID
	"UUID",

	// JSON
	"JSON", "JSONB", "JSONPATH",

	// XML
	"XML",

	// Array (base keyword – actual arrays use TYPE[] syntax)
	"ARRAY",

	// Range
	"INT4RANGE", "INT8RANGE", "NUMRANGE", "TSRANGE", "TSTZRANGE", "DATERANGE",
	"INT4MULTIRANGE", "INT8MULTIRANGE", "NUMMULTIRANGE",
	"TSMULTIRANGE", "TSTZMULTIRANGE", "DATEMULTIRANGE",

	// Object identifiers
	"OID", "REGCLASS", "REGTYPE",
	"REGPROC", "REGPROCEDURE", "REGOPER", "REGOPERATOR",
	"REGCONFIG", "REGDICTIONARY", "REGROLE", "REGNAMESPACE",

	// Special types
	"PG_LSN", "PG_SNAPSHOT", "TXID_SNAPSHOT",

	// Other
	"HSTORE", "LTREE",
	"GEOGRAPHY", "GEOMETRY",
	"RECORD", "CSTRING",
)

var sqliteTypes = toSet(
	"TEXT", "INTEGER", "INT", "REAL", "BLOB", "NUMERIC",
	"BOOLEAN", "BOOL",
	"DATE", "DATETIME", "TIMESTAMP",
	"VARCHAR", "CHAR", "CHARACTER",
	"NCHAR", "NVARCHAR", "CLOB",
	"FLOAT", "DOUBLE", "DOUBLE PRECISION", "DECIMAL",
	"TINYINT", "SMALLINT", "MEDIUMINT", "BIGINT",
	"INT2", "INT8",
	"JSON",
	"ANY",
)

var oracleTypes = toSet(
	// Numeric
	"NUMBER", "FLOAT", "BINARY_FLOAT", "BINARY_DOUBLE",
	"INTEGER", "INT", "SMALLINT", "DECIMAL", "DEC", "NUMERIC",
	"DOUBLE PRECISION", "REAL",

	// Character
	"CHAR", "VARCHAR2", "VARCHAR", "NCHAR", "NVARCHAR2",
	"CLOB", "NCLOB",

	// Binary / LOB
	"BLOB", "RAW", "LONG RAW", "LONG",
	"BFILE",

	// Date / Time
	"DATE",
	"TIMESTAMP", "TIMESTAMP WITH TIME ZONE", "TIMESTAMP WITH LOCAL TIME ZONE",
	"INTERVAL YEAR TO MONTH", "INTERVAL DAY TO SECOND",

	// Other
	"ROWID", "UROWID",
	"XMLTYPE", "JSON",
	"SDO_GEOMETRY", "SDO_TOPO_GEOMETRY", "SDO_GEORASTER",
	"BOOLEAN", // Oracle 23c+

	// Collection types
	"VARRAY", "NESTED TABLE",

	// Object types
	"OBJECT", "REF",

	// Any types
	"ANYDATA", "ANYTYPE", "ANYDATASET",

	// URI types
	"URITYPE",
)

var db2Types = toSet(
	// Numeric
	"SMALLINT", "INTEGER", "INT", "BIGINT",
	"DECIMAL", "DEC", "NUMERIC", "REAL", "FLOAT",
	"DOUBLE", "DOUBLE PRECISION", "DECFLOAT",
	"BOOLEAN",

	// Character
	"CHAR", "CHARACTER", "VARCHAR", "CHARACTER VARYING",
	"CLOB", "GRAPHIC", "VARGRAPHIC", "DBCLOB",
	"NCHAR", "NVARCHAR", "NCLOB",

	// Binary
	"BINARY", "VARBINARY", "BLOB",

	// Date / Time
	"DATE", "TIME", "TIMESTAMP",

	// XML / JSON
	"XML", "JSON",

	// Row types
	"ROWID", "DATALINK",

	// Special types
	"CURSOR", "REFERENCE",
)

var snowflakeTypes = toSet(
	// Numeric
	"NUMBER", "DECIMAL", "DEC", "NUMERIC",
	"INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "BYTEINT",
	"FLOAT", "FLOAT4", "FLOAT8",
	"DOUBLE", "DOUBLE PRECISION", "REAL",
	"BOOLEAN",

	// String
	"VARCHAR", "CHAR", "CHARACTER", "STRING", "TEXT",
	"BINARY", "VARBINARY",

	// Date / Time
	"DATE", "DATETIME",
	"TIME",
	"TIMESTAMP", "TIMESTAMP_LTZ", "TIMESTAMP_NTZ", "TIMESTAMP_TZ",

	// Semi-structured
	"VARIANT", "OBJECT", "ARRAY",

	// Structured
	"MAP",

	// UUID
	"UUID",

	// Geospatial
	"GEOGRAPHY", "GEOMETRY",

	// Vector
	"VECTOR",

	// Other
	"DECFLOAT",
)

var mssqlTypes = toSet(
	// Exact numeric
	"BIGINT", "INT", "INTEGER", "SMALLINT", "TINYINT",
	"BIT", "DECIMAL", "DEC", "NUMERIC",
	"MONEY", "SMALLMONEY",

	// Approximate numeric
	"FLOAT", "REAL",

	// Date / Time
	"DATE", "DATETIME", "DATETIME2", "DATETIMEOFFSET",
	"SMALLDATETIME", "TIME",

	// Character
	"CHAR", "VARCHAR", "TEXT",
	"NCHAR", "NVARCHAR", "NTEXT",

	// Binary
	"BINARY", "VARBINARY", "IMAGE",

	// Other
	"XML", "JSON",
	"UNIQUEIDENTIFIER",
	"SQL_VARIANT",
	"GEOGRAPHY", "GEOMETRY",
	"HIERARCHYID",
	"ROWVERSION", "TIMESTAMP",
	"CURSOR", "TABLE", "SYSNAME",
)

// ValidateRawType checks whether rawType is a valid SQL type for the
// given dialect. It returns nil when the type is valid.
// A descriptive error is returned when the type is unrecognized.
func ValidateRawType(rawType string, dialect Dialect) error {
	if strings.TrimSpace(rawType) == "" {
		return errors.New("raw_type is empty")
	}

	types, ok := dialectRawTypes[dialect]
	if !ok {
		return fmt.Errorf("unknown dialect: %q", dialect)
	}

	base := normalizeRawTypeBase(rawType)
	if base == "" {
		return fmt.Errorf("raw_type %q could not be normalized to a base type", rawType)
	}

	if _, ok := types[base]; ok {
		return nil
	}

	return fmt.Errorf(
		"raw_type %q (resolved base: %q) is not a valid type for dialect %q; valid types: %s",
		rawType, base, string(dialect), validTypesList(dialect),
	)
}

// toSet builds a case-insensitive lookup set from a variadic list of
// upper-cased type names.
func toSet(names ...string) map[string]struct{} {
	m := make(map[string]struct{}, len(names))
	for _, n := range names {
		m[strings.ToUpper(n)] = struct{}{}
	}
	return m
}

// mergeSets combines multiple type sets into a single set.
func mergeSets(sets ...map[string]struct{}) map[string]struct{} {
	result := make(map[string]struct{})
	for _, s := range sets {
		maps.Copy(result, s)
	}
	return result
}

// normalizeRawTypeBase extracts the base type name from a raw SQL type
// string. It removes parenthesized portions (length, precision, enum
// values, etc.), collapses whitespace, and uppercases the result.
//
// Examples:
//
//	"varchar(255)"                     -> "VARCHAR"
//	"TIMESTAMP(6) WITH TIME ZONE"      -> "TIMESTAMP WITH TIME ZONE"
//	"enum('a','b','c')"                -> "ENUM"
//	"DOUBLE PRECISION"                 -> "DOUBLE PRECISION"
//	"INT UNSIGNED"                     -> "INT"
func normalizeRawTypeBase(rawType string) string {
	base := parenRe.ReplaceAllString(rawType, "")

	base = strings.ToUpper(strings.TrimSpace(base))

	base = stripModifiers(base)

	base = wsRe.ReplaceAllString(strings.TrimSpace(base), " ")

	return base
}

// stripModifiers removes trailing SQL modifiers that are not part of the
// type name. We must be careful not to strip words that are part of
// multi-word types (e.g. "VARYING" in "CHARACTER VARYING"). The approach:
// only strip a modifier when it is NOT part of a known multi-word type.
func stripModifiers(s string) string {
	s = modifierUnsignedRe.ReplaceAllString(s, "")
	s = modifierSignedRe.ReplaceAllString(s, "")
	s = modifierZerofillRe.ReplaceAllString(s, "")

	return strings.TrimSpace(s)
}

// validTypesList returns a sorted, comma-separated string of all valid
// base types for a dialect. Used only for error messages.
func validTypesList(d Dialect) string {
	types, ok := dialectRawTypes[d]
	if !ok {
		return "(unknown dialect)"
	}
	names := make([]string, 0, len(types))
	for t := range types {
		names = append(names, t)
	}
	sort.Strings(names)
	return strings.Join(names, ", ")
}
