package core

import (
	"strings"
	"testing"
)

func TestNormalizeRawTypeBase(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		// Simple types
		{"INT", "INT"},
		{"int", "INT"},
		{"varchar", "VARCHAR"},
		{"VARCHAR", "VARCHAR"},

		// With length / precision
		{"VARCHAR(255)", "VARCHAR"},
		{"varchar(255)", "VARCHAR"},
		{"DECIMAL(10,2)", "DECIMAL"},
		{"NUMERIC(18, 4)", "NUMERIC"},
		{"CHAR(1)", "CHAR"},
		{"BIT(8)", "BIT"},

		// Multi-word types
		{"DOUBLE PRECISION", "DOUBLE PRECISION"},
		{"double precision", "DOUBLE PRECISION"},
		{"CHARACTER VARYING", "CHARACTER VARYING"},

		// Multi-word with parenthesized parameters
		{"TIMESTAMP(6) WITH TIME ZONE", "TIMESTAMP WITH TIME ZONE"},
		{"timestamp(3) with time zone", "TIMESTAMP WITH TIME ZONE"},
		{"TIMESTAMP WITHOUT TIME ZONE", "TIMESTAMP WITHOUT TIME ZONE"},
		{"INTERVAL YEAR TO MONTH", "INTERVAL YEAR TO MONTH"},
		{"INTERVAL DAY TO SECOND", "INTERVAL DAY TO SECOND"},
		{"TIMESTAMP WITH LOCAL TIME ZONE", "TIMESTAMP WITH LOCAL TIME ZONE"},

		// Enum with values
		{"enum('a','b','c')", "ENUM"},
		{"ENUM('free','pro','enterprise')", "ENUM"},

		// With UNSIGNED / SIGNED / ZEROFILL modifiers
		{"INT UNSIGNED", "INT"},
		{"BIGINT UNSIGNED", "BIGINT"},
		{"TINYINT(1) UNSIGNED", "TINYINT"},
		{"int unsigned", "INT"},
		{"MEDIUMINT UNSIGNED ZEROFILL", "MEDIUMINT"},
		{"FLOAT SIGNED", "FLOAT"},

		// Whitespace handling
		{"  VARCHAR(255)  ", "VARCHAR"},
		{"  int  ", "INT"},
		{"  DOUBLE   PRECISION  ", "DOUBLE PRECISION"},

		// Edge: nested or multiple parens (shouldn't happen but be safe)
		{"NUMERIC(10)", "NUMERIC"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := normalizeRawTypeBase(tt.input)
			if got != tt.want {
				t.Errorf("normalizeRawTypeBase(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

// assertValidRawTypes is a test helper that asserts every rawType is accepted
// by ValidateRawType for the given dialect.
func assertValidRawTypes(t *testing.T, dialect Dialect, rawTypes []string) {
	t.Helper()
	for _, rt := range rawTypes {
		t.Run(rt, func(t *testing.T) {
			if err := ValidateRawType(rt, new(dialect)); err != nil {
				t.Errorf("ValidateRawType(%q, %q) returned error: %v", rt, dialect, err)
			}
		})
	}
}

func TestValidateRawTypeValidMySQL(t *testing.T) {
	assertValidRawTypes(t, DialectMySQL, []string{
		"VARCHAR(255)", "INT", "BIGINT UNSIGNED", "TINYINT(1)",
		"ENUM('a','b')", "JSON", "DATETIME", "TIMESTAMP",
		"DECIMAL(10,2)", "MEDIUMTEXT", "LONGBLOB", "DOUBLE PRECISION",
		"GEOMETRY", "SET", "BOOLEAN", "YEAR",
		"varchar(255)", // case insensitivity
	})
}

func TestValidateRawTypeValidMariaDB(t *testing.T) {
	assertValidRawTypes(t, DialectMariaDB, []string{
		"VARCHAR(255)", "UUID", "INET4", "INET6",
		"JSON", "BIGINT UNSIGNED",
	})
}

func TestValidateRawTypeValidPostgreSQL(t *testing.T) {
	assertValidRawTypes(t, DialectPostgreSQL, []string{
		"VARCHAR(255)", "TEXT", "INTEGER", "BIGSERIAL",
		"JSONB", "UUID", "BOOLEAN", "BYTEA",
		"TIMESTAMP WITH TIME ZONE", "TIMESTAMP(6) WITH TIME ZONE",
		"TIMESTAMPTZ", "TIMESTAMP WITHOUT TIME ZONE",
		"TIME WITH TIME ZONE", "INTERVAL",
		"CIDR", "INET", "MACADDR", "TSVECTOR", "INT4RANGE",
		"DOUBLE PRECISION", "CHARACTER VARYING", "BIT VARYING",
		"SERIAL", "MONEY", "HSTORE", "XML",
		"jsonb", // case insensitivity
	})
}

func TestValidateRawTypeValidSQLite(t *testing.T) {
	assertValidRawTypes(t, DialectSQLite, []string{
		"TEXT", "INTEGER", "REAL", "BLOB",
		"NUMERIC", "VARCHAR", "BOOLEAN", "JSON",
	})
}

func TestValidateRawTypeValidOracle(t *testing.T) {
	assertValidRawTypes(t, DialectOracle, []string{
		"NUMBER", "VARCHAR2(255)", "CLOB", "BLOB", "DATE",
		"TIMESTAMP WITH TIME ZONE", "TIMESTAMP WITH LOCAL TIME ZONE",
		"INTERVAL YEAR TO MONTH", "INTERVAL DAY TO SECOND",
		"NVARCHAR2(100)", "RAW", "LONG RAW",
		"BINARY_FLOAT", "BINARY_DOUBLE",
		"ROWID", "XMLTYPE", "BOOLEAN",
		"number(10,2)", // case insensitivity
	})
}

func TestValidateRawTypeValidDB2(t *testing.T) {
	assertValidRawTypes(t, DialectDB2, []string{
		"SMALLINT", "INTEGER", "BIGINT", "DECIMAL(10,2)",
		"VARCHAR(255)", "CLOB", "BLOB", "DATE", "TIMESTAMP",
		"XML", "DECFLOAT", "GRAPHIC", "VARGRAPHIC", "DBCLOB",
		"CHARACTER VARYING",
	})
}

func TestValidateRawTypeValidSnowflake(t *testing.T) {
	assertValidRawTypes(t, DialectSnowflake, []string{
		"VARCHAR(255)", "NUMBER(38,0)", "BOOLEAN",
		"TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "TIMESTAMP_TZ",
		"VARIANT", "OBJECT", "ARRAY", "GEOGRAPHY",
		"STRING", "BYTEINT", "VECTOR",
		"timestamp_ntz", // case insensitivity
	})
}

func TestValidateRawTypeValidMSSQL(t *testing.T) {
	assertValidRawTypes(t, DialectMSSQL, []string{
		"INT", "BIGINT", "VARCHAR(255)", "NVARCHAR(100)",
		"DATETIME2", "DATETIMEOFFSET", "UNIQUEIDENTIFIER",
		"XML", "MONEY", "SMALLMONEY", "SQL_VARIANT",
		"HIERARCHYID", "GEOGRAPHY", "GEOMETRY",
		"VARBINARY(MAX)", "IMAGE", "BIT",
		"uniqueidentifier", // case insensitivity
	})
}

func TestValidateRawTypeInvalid(t *testing.T) {
	tests := []struct {
		rawType string
		dialect Dialect
	}{
		// Types that don't exist in the dialect
		{"JSONB", DialectMySQL},
		{"UUID", DialectMySQL},
		{"SERIAL", DialectMySQL},
		{"BYTEA", DialectMySQL},
		{"VARCHAR2(255)", DialectMySQL},
		{"NUMBER", DialectMySQL},

		{"JSONB", DialectMariaDB},
		{"SERIAL", DialectMariaDB},
		{"BYTEA", DialectMariaDB},

		{"MEDIUMTEXT", DialectPostgreSQL},
		{"LONGBLOB", DialectPostgreSQL},
		{"YEAR", DialectPostgreSQL},
		{"TINYINT", DialectPostgreSQL},
		{"SET", DialectPostgreSQL},
		{"VARCHAR2", DialectPostgreSQL},

		{"JSONB", DialectSQLite},
		{"UUID", DialectSQLite},
		{"SERIAL", DialectSQLite},
		{"BYTEA", DialectSQLite},

		{"VARCHAR2", DialectDB2},
		{"TINYINT", DialectDB2},
		{"JSONB", DialectDB2},

		{"JSONB", DialectSnowflake},
		{"SERIAL", DialectSnowflake},
		{"CLOB", DialectSnowflake},
		{"MEDIUMTEXT", DialectSnowflake},

		{"SERIAL", DialectMSSQL},
		{"JSONB", DialectMSSQL},
		{"BOOLEAN", DialectMSSQL},
		{"BYTEA", DialectMSSQL},
		{"MEDIUMTEXT", DialectMSSQL},

		// Completely made-up types
		{"SUPERTEXT", DialectMySQL},
		{"MEGAINT", DialectPostgreSQL},
		{"FOOBAR", DialectSQLite},
		{"WIDGETVECTOR", DialectSnowflake},
	}

	for _, tt := range tests {
		t.Run(string(tt.dialect)+"/"+tt.rawType, func(t *testing.T) {
			err := ValidateRawType(tt.rawType, new(tt.dialect))
			if err == nil {
				t.Errorf("ValidateRawType(%q, %q) returned nil, want error", tt.rawType, tt.dialect)
			}
			// Error should mention the raw type
			if err != nil && !strings.Contains(err.Error(), tt.rawType) {
				t.Errorf("error message should mention the raw type %q, got: %v", tt.rawType, err)
			}
			// Error should mention the dialect
			if err != nil && !strings.Contains(err.Error(), string(tt.dialect)) {
				t.Errorf("error message should mention the dialect %q, got: %v", tt.dialect, err)
			}
		})
	}
}

func TestValidateRawTypeEmptyRawType(t *testing.T) {
	d := DialectMySQL
	tests := []string{"", "   ", "\t"}
	for _, rt := range tests {
		t.Run("empty_"+rt, func(t *testing.T) {
			err := ValidateRawType(rt, &d)
			if err == nil {
				t.Errorf("ValidateRawType(%q, mysql) should return error for empty input", rt)
			}
		})
	}
}

func TestAllDialectsHaveRawTypes(t *testing.T) {
	for _, d := range SupportedDialects() {
		dialect := d
		if _, ok := dialectRawTypes[dialect]; !ok {
			t.Errorf("dialect %q has no entry in dialectRawTypes", d)
		}
	}
}
