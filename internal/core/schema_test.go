package core

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDatabaseFindTable(t *testing.T) {
	db := &Database{
		Name: "testdb",
		Tables: []*Table{
			{Name: "users"},
			{Name: "orders"},
			{Name: "products"},
		},
	}

	t.Run("find existing table", func(t *testing.T) {
		table := db.FindTable("users")
		assert.NotNil(t, table)
		assert.Equal(t, "users", table.Name)
	})

	t.Run("table not found", func(t *testing.T) {
		table := db.FindTable("nonexistent")
		assert.Nil(t, table)
	})

	t.Run("empty database", func(t *testing.T) {
		emptyDB := &Database{Name: "empty"}
		table := emptyDB.FindTable("users")
		assert.Nil(t, table)
	})
}

func TestTableFindColumn(t *testing.T) {
	table := &Table{
		Name: "users",
		Columns: []*Column{
			{Name: "id"},
			{Name: "email"},
			{Name: "created_at"},
		},
	}

	t.Run("find existing column", func(t *testing.T) {
		col := table.FindColumn("id")
		assert.NotNil(t, col)
		assert.Equal(t, "id", col.Name)
	})

	t.Run("column not found", func(t *testing.T) {
		col := table.FindColumn("nonexistent")
		assert.Nil(t, col)
	})

	t.Run("empty table", func(t *testing.T) {
		emptyTable := &Table{Name: "empty"}
		col := emptyTable.FindColumn("id")
		assert.Nil(t, col)
	})
}

func TestTableFindConstraint(t *testing.T) {
	table := &Table{
		Name: "users",
		Constraints: []*Constraint{
			{Name: "pk_users", Type: ConstraintPrimaryKey},
			{Name: "fk_orders", Type: ConstraintForeignKey},
			{Name: "uq_email", Type: ConstraintUnique},
		},
	}

	t.Run("find existing constraint", func(t *testing.T) {
		c := table.FindConstraint("pk_users")
		assert.NotNil(t, c)
		assert.Equal(t, "pk_users", c.Name)
	})

	t.Run("constraint not found", func(t *testing.T) {
		c := table.FindConstraint("nonexistent")
		assert.Nil(t, c)
	})

	t.Run("empty table", func(t *testing.T) {
		emptyTable := &Table{Name: "empty"}
		c := emptyTable.FindConstraint("pk")
		assert.Nil(t, c)
	})
}

func TestTableFindIndex(t *testing.T) {
	table := &Table{
		Name: "users",
		Indexes: []*Index{
			{Name: "idx_email"},
			{Name: "idx_name"},
			{Name: "idx_created"},
		},
	}

	t.Run("find existing index", func(t *testing.T) {
		idx := table.FindIndex("idx_email")
		assert.NotNil(t, idx)
		assert.Equal(t, "idx_email", idx.Name)
	})

	t.Run("index not found", func(t *testing.T) {
		idx := table.FindIndex("nonexistent")
		assert.Nil(t, idx)
	})

	t.Run("empty table", func(t *testing.T) {
		emptyTable := &Table{Name: "empty"}
		idx := emptyTable.FindIndex("idx")
		assert.Nil(t, idx)
	})
}

func TestTablePrimaryKey(t *testing.T) {
	t.Run("table with primary key", func(t *testing.T) {
		table := &Table{
			Name: "users",
			Constraints: []*Constraint{
				{Name: "uq_email", Type: ConstraintUnique},
				{Name: "pk_users", Type: ConstraintPrimaryKey, Columns: []string{"id"}},
				{Name: "fk_orders", Type: ConstraintForeignKey},
			},
		}
		pk := table.PrimaryKey()
		assert.NotNil(t, pk)
		assert.Equal(t, "pk_users", pk.Name)
		assert.Equal(t, ConstraintPrimaryKey, pk.Type)
	})

	t.Run("table without primary key", func(t *testing.T) {
		table := &Table{
			Name: "users",
			Constraints: []*Constraint{
				{Name: "uq_email", Type: ConstraintUnique},
				{Name: "fk_orders", Type: ConstraintForeignKey},
			},
		}
		pk := table.PrimaryKey()
		assert.Nil(t, pk)
	})

	t.Run("table with no constraints", func(t *testing.T) {
		table := &Table{Name: "users"}
		pk := table.PrimaryKey()
		assert.Nil(t, pk)
	})
}

func TestIndexNames(t *testing.T) {
	t.Run("index with multiple columns", func(t *testing.T) {
		idx := &Index{
			Name: "idx_composite",
			Columns: []ColumnIndex{
				{Name: "first_name"},
				{Name: "last_name"},
				{Name: "email"},
			},
		}
		names := idx.Names()
		assert.Equal(t, []string{"first_name", "last_name", "email"}, names)
	})

	t.Run("index with single column", func(t *testing.T) {
		idx := &Index{
			Name: "idx_email",
			Columns: []ColumnIndex{
				{Name: "email"},
			},
		}
		names := idx.Names()
		assert.Equal(t, []string{"email"}, names)
	})

	t.Run("index with no columns", func(t *testing.T) {
		idx := &Index{Name: "idx_empty"}
		names := idx.Names()
		assert.Equal(t, []string{}, names)
	})
}

func TestTableString(t *testing.T) {
	t.Run("table with all components", func(t *testing.T) {
		table := &Table{
			Name: "users",
			Columns: []*Column{
				{Name: "id"},
				{Name: "email"},
				{Name: "name"},
			},
			Constraints: []*Constraint{
				{Name: "pk_users"},
				{Name: "uq_email"},
			},
			Indexes: []*Index{
				{Name: "idx_name"},
			},
		}
		str := table.String()
		assert.Equal(t, "Table: users (3 cols, 2 constraints, 1 indexes)", str)
	})

	t.Run("table with no components", func(t *testing.T) {
		table := &Table{Name: "empty"}
		str := table.String()
		assert.Equal(t, "Table: empty (0 cols, 0 constraints, 0 indexes)", str)
	})
}

func TestNormalizeDataType(t *testing.T) {
	tests := []struct {
		name     string
		rawType  string
		expected DataType
	}{
		// String types
		{"varchar", "VARCHAR(255)", DataTypeString},
		{"char", "CHAR(10)", DataTypeString},
		{"text", "TEXT", DataTypeString},
		{"longtext", "LONGTEXT", DataTypeString},
		{"mediumtext", "MEDIUMTEXT", DataTypeString},
		{"tinytext", "TINYTEXT", DataTypeString},
		{"string", "STRING", DataTypeString},
		{"set", "SET('x','y','z')", DataTypeString},

		// Enum types
		{"enum", "ENUM('a','b','c')", DataTypeEnum},

		// Boolean types
		{"boolean", "BOOLEAN", DataTypeBoolean},
		{"bool", "BOOL", DataTypeBoolean},
		{"tinyint1", "TINYINT(1)", DataTypeBoolean},

		// Integer types
		{"int", "INT", DataTypeInt},
		{"integer", "INTEGER", DataTypeInt},
		{"bigint", "BIGINT", DataTypeInt},
		{"smallint", "SMALLINT", DataTypeInt},
		{"mediumint", "MEDIUMINT", DataTypeInt},
		{"tinyint", "TINYINT(4)", DataTypeInt},

		// Float types
		{"float", "FLOAT", DataTypeFloat},
		{"double", "DOUBLE", DataTypeFloat},
		{"decimal", "DECIMAL(10,2)", DataTypeFloat},
		{"numeric", "NUMERIC(8,4)", DataTypeFloat},
		{"real", "REAL", DataTypeFloat},

		// Datetime types
		{"timestamp", "TIMESTAMP", DataTypeDatetime},
		{"datetime", "DATETIME", DataTypeDatetime},
		{"date", "DATE", DataTypeDatetime},
		{"time", "TIME", DataTypeDatetime},

		// JSON type
		{"json", "JSON", DataTypeJSON},

		// UUID type
		{"uuid", "UUID", DataTypeUUID},

		// Binary types
		{"blob", "BLOB", DataTypeBinary},
		{"binary", "BINARY(16)", DataTypeBinary},
		{"varbinary", "VARBINARY(255)", DataTypeBinary},
		{"longblob", "LONGBLOB", DataTypeBinary},

		// Unknown types
		{"unknown", "GEOMETRY", DataTypeUnknown},
		{"custom", "CUSTOM_TYPE", DataTypeUnknown},

		// Edge cases
		{"lowercase", "varchar(100)", DataTypeString},
		{"mixed case", "VarChar(50)", DataTypeString},
		{"with spaces", "  INT  ", DataTypeInt},
		{"empty", "", DataTypeUnknown},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := NormalizeDataType(tt.rawType)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestDataTypeConstants(t *testing.T) {
	assert.Equal(t, DataTypeString, DataType("string"))
	assert.Equal(t, DataTypeInt, DataType("int"))
	assert.Equal(t, DataTypeFloat, DataType("float"))
	assert.Equal(t, DataTypeBoolean, DataType("boolean"))
	assert.Equal(t, DataTypeDatetime, DataType("datetime"))
	assert.Equal(t, DataTypeJSON, DataType("json"))
	assert.Equal(t, DataTypeUUID, DataType("uuid"))
	assert.Equal(t, DataTypeBinary, DataType("binary"))
	assert.Equal(t, DataTypeEnum, DataType("enum"))
	assert.Equal(t, DataTypeUnknown, DataType("unknown"))
}

func TestDialectConstants(t *testing.T) {
	assert.Equal(t, DialectMySQL, Dialect("mysql"))
	assert.Equal(t, DialectMariaDB, Dialect("mariadb"))
	assert.Equal(t, DialectPostgreSQL, Dialect("postgresql"))
	assert.Equal(t, DialectSQLite, Dialect("sqlite"))
	assert.Equal(t, DialectOracle, Dialect("oracle"))
	assert.Equal(t, DialectDB2, Dialect("db2"))
	assert.Equal(t, DialectSnowflake, Dialect("snowflake"))
	assert.Equal(t, DialectMSSQL, Dialect("mssql"))
}

func TestSupportedDialects(t *testing.T) {
	dialects := SupportedDialects()
	assert.Len(t, dialects, 9)
	assert.Contains(t, dialects, DialectMySQL)
	assert.Contains(t, dialects, DialectMariaDB)
	assert.Contains(t, dialects, DialectPostgreSQL)
	assert.Contains(t, dialects, DialectSQLite)
	assert.Contains(t, dialects, DialectOracle)
	assert.Contains(t, dialects, DialectDB2)
	assert.Contains(t, dialects, DialectSnowflake)
	assert.Contains(t, dialects, DialectMSSQL)
}

func TestValidDialect(t *testing.T) {
	t.Run("valid dialects", func(t *testing.T) {
		for _, d := range SupportedDialects() {
			assert.True(t, ValidDialect(string(d)), "expected %q to be valid", d)
		}
	})

	t.Run("case insensitive", func(t *testing.T) {
		assert.True(t, ValidDialect("MySQL"))
		assert.True(t, ValidDialect("POSTGRESQL"))
		assert.True(t, ValidDialect("Snowflake"))
	})

	t.Run("invalid dialects", func(t *testing.T) {
		assert.False(t, ValidDialect(""))
		assert.False(t, ValidDialect("mongo"))
		assert.False(t, ValidDialect("redis"))
		assert.False(t, ValidDialect("cassandra"))
	})
}

func TestParseReferences(t *testing.T) {
	t.Run("valid reference", func(t *testing.T) {
		tbl, col, ok := ParseReferences("tenants.id")
		assert.True(t, ok)
		assert.Equal(t, "tenants", tbl)
		assert.Equal(t, "id", col)
	})

	t.Run("valid reference with schema prefix", func(t *testing.T) {
		tbl, col, ok := ParseReferences("public.users.id")
		assert.True(t, ok)
		assert.Equal(t, "public.users", tbl)
		assert.Equal(t, "id", col)
	})

	t.Run("empty string", func(t *testing.T) {
		_, _, ok := ParseReferences("")
		assert.False(t, ok)
	})

	t.Run("no dot", func(t *testing.T) {
		_, _, ok := ParseReferences("tenants")
		assert.False(t, ok)
	})

	t.Run("dot at start", func(t *testing.T) {
		_, _, ok := ParseReferences(".id")
		assert.False(t, ok)
	})

	t.Run("dot at end", func(t *testing.T) {
		_, _, ok := ParseReferences("tenants.")
		assert.False(t, ok)
	})

	t.Run("whitespace trimmed", func(t *testing.T) {
		tbl, col, ok := ParseReferences("  tenants.id  ")
		assert.True(t, ok)
		assert.Equal(t, "tenants", tbl)
		assert.Equal(t, "id", col)
	})
}

func TestBuildEnumTypeRaw(t *testing.T) {
	t.Run("basic values", func(t *testing.T) {
		result := BuildEnumTypeRaw([]string{"free", "pro", "enterprise"})
		assert.Equal(t, "enum('free','pro','enterprise')", result)
	})

	t.Run("single value", func(t *testing.T) {
		result := BuildEnumTypeRaw([]string{"active"})
		assert.Equal(t, "enum('active')", result)
	})

	t.Run("empty values", func(t *testing.T) {
		result := BuildEnumTypeRaw([]string{})
		assert.Equal(t, "enum()", result)
	})

	t.Run("nil values", func(t *testing.T) {
		result := BuildEnumTypeRaw(nil)
		assert.Equal(t, "enum()", result)
	})

	t.Run("values with single quotes escaped", func(t *testing.T) {
		result := BuildEnumTypeRaw([]string{"it's", "they're"})
		assert.Equal(t, "enum('it''s','they''re')", result)
	})
}

func TestAutoGenerateConstraintName(t *testing.T) {
	t.Run("primary key", func(t *testing.T) {
		name := AutoGenerateConstraintName(ConstraintPrimaryKey, "Users", []string{"id"}, "")
		assert.Equal(t, "pk_users", name)
	})

	t.Run("unique", func(t *testing.T) {
		name := AutoGenerateConstraintName(ConstraintUnique, "Users", []string{"email"}, "")
		assert.Equal(t, "uq_users_email", name)
	})

	t.Run("check", func(t *testing.T) {
		name := AutoGenerateConstraintName(ConstraintCheck, "Users", []string{"age"}, "")
		assert.Equal(t, "chk_users_age", name)
	})

	t.Run("foreign key", func(t *testing.T) {
		name := AutoGenerateConstraintName(ConstraintForeignKey, "Orders", []string{"user_id"}, "Users")
		assert.Equal(t, "fk_orders_users", name)
	})

	t.Run("composite unique", func(t *testing.T) {
		name := AutoGenerateConstraintName(ConstraintUnique, "Roles", []string{"tenant_id", "name"}, "")
		assert.Equal(t, "uq_roles_tenant_id_name", name)
	})
}

func TestColumnHasIdentityOptions(t *testing.T) {
	t.Run("no identity options", func(t *testing.T) {
		col := &Column{Name: "id", RawType: "bigint", AutoIncrement: true}
		assert.False(t, col.HasIdentityOptions())
	})

	t.Run("with seed only", func(t *testing.T) {
		col := &Column{Name: "id", RawType: "bigint", AutoIncrement: true, IdentitySeed: 100}
		assert.True(t, col.HasIdentityOptions())
	})

	t.Run("with increment only", func(t *testing.T) {
		col := &Column{Name: "id", RawType: "bigint", AutoIncrement: true, IdentityIncrement: 5}
		assert.True(t, col.HasIdentityOptions())
	})

	t.Run("with both seed and increment", func(t *testing.T) {
		col := &Column{Name: "id", RawType: "bigint", AutoIncrement: true, IdentitySeed: 1000, IdentityIncrement: 10}
		assert.True(t, col.HasIdentityOptions())
	})
}

func TestGenerationStorageConstants(t *testing.T) {
	assert.Equal(t, GenerationVirtual, GenerationStorage("VIRTUAL"))
	assert.Equal(t, GenerationStored, GenerationStorage("STORED"))
}

func TestConstraintTypeConstants(t *testing.T) {
	assert.Equal(t, ConstraintPrimaryKey, ConstraintType("PRIMARY KEY"))
	assert.Equal(t, ConstraintForeignKey, ConstraintType("FOREIGN KEY"))
	assert.Equal(t, ConstraintUnique, ConstraintType("UNIQUE"))
	assert.Equal(t, ConstraintCheck, ConstraintType("CHECK"))
}

func TestReferentialActionConstants(t *testing.T) {
	assert.Equal(t, RefActionNone, ReferentialAction(""))
	assert.Equal(t, RefActionCascade, ReferentialAction("CASCADE"))
	assert.Equal(t, RefActionRestrict, ReferentialAction("RESTRICT"))
	assert.Equal(t, RefActionSetNull, ReferentialAction("SET NULL"))
	assert.Equal(t, RefActionSetDefault, ReferentialAction("SET DEFAULT"))
	assert.Equal(t, RefActionNoAction, ReferentialAction("NO ACTION"))
}

func TestIndexTypeConstants(t *testing.T) {
	assert.Equal(t, IndexTypeBTree, IndexType("BTREE"))
	assert.Equal(t, IndexTypeHash, IndexType("HASH"))
	assert.Equal(t, IndexTypeFullText, IndexType("FULLTEXT"))
	assert.Equal(t, IndexTypeSpatial, IndexType("SPATIAL"))
	assert.Equal(t, IndexTypeGIN, IndexType("GIN"))
	assert.Equal(t, IndexTypeGiST, IndexType("GiST"))
}

func TestIndexVisibilityConstants(t *testing.T) {
	assert.Equal(t, IndexVisible, IndexVisibility("VISIBLE"))
	assert.Equal(t, IndexInvisible, IndexVisibility("INVISIBLE"))
}

func TestSortOrderConstants(t *testing.T) {
	assert.Equal(t, SortAsc, SortOrder("ASC"))
	assert.Equal(t, SortDesc, SortOrder("DESC"))
}
