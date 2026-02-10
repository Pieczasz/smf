package core

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTableGetName(t *testing.T) {
	table := &Table{Name: "users"}
	assert.Equal(t, "users", table.GetName())
}

func TestColumnGetName(t *testing.T) {
	column := &Column{Name: "id"}
	assert.Equal(t, "id", column.GetName())
}

func TestConstraintGetName(t *testing.T) {
	constraint := &Constraint{Name: "pk_users"}
	assert.Equal(t, "pk_users", constraint.GetName())
}

func TestIndexGetName(t *testing.T) {
	index := &Index{Name: "idx_users_email"}
	assert.Equal(t, "idx_users_email", index.GetName())
}

func TestDatabaseFindTable(t *testing.T) {
	db := &Database{
		Name: "testdb",
		Tables: []*Table{
			{Name: "users"},
			{Name: "orders"},
			{Name: "Products"},
		},
	}

	t.Run("find existing table", func(t *testing.T) {
		table := db.FindTable("users")
		assert.NotNil(t, table)
		assert.Equal(t, "users", table.Name)
	})

	t.Run("find existing table case insensitive", func(t *testing.T) {
		table := db.FindTable("USERS")
		assert.NotNil(t, table)
		assert.Equal(t, "users", table.Name)
	})

	t.Run("find existing table mixed case", func(t *testing.T) {
		table := db.FindTable("products")
		assert.NotNil(t, table)
		assert.Equal(t, "Products", table.Name)
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
			{Name: "Email"},
			{Name: "created_at"},
		},
	}

	t.Run("find existing column", func(t *testing.T) {
		col := table.FindColumn("id")
		assert.NotNil(t, col)
		assert.Equal(t, "id", col.Name)
	})

	t.Run("find existing column case insensitive", func(t *testing.T) {
		col := table.FindColumn("ID")
		assert.NotNil(t, col)
		assert.Equal(t, "id", col.Name)
	})

	t.Run("find existing column mixed case", func(t *testing.T) {
		col := table.FindColumn("email")
		assert.NotNil(t, col)
		assert.Equal(t, "Email", col.Name)
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
			{Name: "FK_Orders", Type: ConstraintForeignKey},
			{Name: "uq_email", Type: ConstraintUnique},
		},
	}

	t.Run("find existing constraint", func(t *testing.T) {
		c := table.FindConstraint("pk_users")
		assert.NotNil(t, c)
		assert.Equal(t, "pk_users", c.Name)
	})

	t.Run("find existing constraint case insensitive", func(t *testing.T) {
		c := table.FindConstraint("PK_USERS")
		assert.NotNil(t, c)
		assert.Equal(t, "pk_users", c.Name)
	})

	t.Run("find existing constraint mixed case", func(t *testing.T) {
		c := table.FindConstraint("fk_orders")
		assert.NotNil(t, c)
		assert.Equal(t, "FK_Orders", c.Name)
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
			{Name: "IDX_Name"},
			{Name: "idx_created"},
		},
	}

	t.Run("find existing index", func(t *testing.T) {
		idx := table.FindIndex("idx_email")
		assert.NotNil(t, idx)
		assert.Equal(t, "idx_email", idx.Name)
	})

	t.Run("find existing index case insensitive", func(t *testing.T) {
		idx := table.FindIndex("IDX_EMAIL")
		assert.NotNil(t, idx)
		assert.Equal(t, "idx_email", idx.Name)
	})

	t.Run("find existing index mixed case", func(t *testing.T) {
		idx := table.FindIndex("idx_name")
		assert.NotNil(t, idx)
		assert.Equal(t, "IDX_Name", idx.Name)
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
			Columns: []IndexColumn{
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
			Columns: []IndexColumn{
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
	assert.Equal(t, DataType("string"), DataTypeString)
	assert.Equal(t, DataType("int"), DataTypeInt)
	assert.Equal(t, DataType("float"), DataTypeFloat)
	assert.Equal(t, DataType("boolean"), DataTypeBoolean)
	assert.Equal(t, DataType("datetime"), DataTypeDatetime)
	assert.Equal(t, DataType("json"), DataTypeJSON)
	assert.Equal(t, DataType("uuid"), DataTypeUUID)
	assert.Equal(t, DataType("binary"), DataTypeBinary)
	assert.Equal(t, DataType("enum"), DataTypeEnum)
	assert.Equal(t, DataType("unknown"), DataTypeUnknown)
}

func TestDialectConstants(t *testing.T) {
	assert.Equal(t, Dialect("mysql"), DialectMySQL)
	assert.Equal(t, Dialect("mariadb"), DialectMariaDB)
	assert.Equal(t, Dialect("postgresql"), DialectPostgreSQL)
	assert.Equal(t, Dialect("sqlite"), DialectSQLite)
	assert.Equal(t, Dialect("oracle"), DialectOracle)
	assert.Equal(t, Dialect("db2"), DialectDB2)
	assert.Equal(t, Dialect("snowflake"), DialectSnowflake)
	assert.Equal(t, Dialect("mssql"), DialectMSSQL)
}

func TestSupportedDialects(t *testing.T) {
	dialects := SupportedDialects()
	assert.Len(t, dialects, 8)
	assert.Contains(t, dialects, DialectMySQL)
	assert.Contains(t, dialects, DialectMariaDB)
	assert.Contains(t, dialects, DialectPostgreSQL)
	assert.Contains(t, dialects, DialectSQLite)
	assert.Contains(t, dialects, DialectOracle)
	assert.Contains(t, dialects, DialectDB2)
	assert.Contains(t, dialects, DialectSnowflake)
	assert.Contains(t, dialects, DialectMSSQL)
}

func TestIsValidDialect(t *testing.T) {
	t.Run("valid dialects", func(t *testing.T) {
		for _, d := range SupportedDialects() {
			assert.True(t, IsValidDialect(string(d)), "expected %q to be valid", d)
		}
	})

	t.Run("case insensitive", func(t *testing.T) {
		assert.True(t, IsValidDialect("MySQL"))
		assert.True(t, IsValidDialect("POSTGRESQL"))
		assert.True(t, IsValidDialect("Snowflake"))
	})

	t.Run("invalid dialects", func(t *testing.T) {
		assert.False(t, IsValidDialect(""))
		assert.False(t, IsValidDialect("mongo"))
		assert.False(t, IsValidDialect("redis"))
		assert.False(t, IsValidDialect("cassandra"))
	})
}

func TestColumnHasTypeOverride(t *testing.T) {
	t.Run("no overrides", func(t *testing.T) {
		col := &Column{Name: "id", TypeRaw: "bigint"}
		assert.False(t, col.HasTypeOverride("mysql"))
		assert.False(t, col.HasTypeOverride(""))
	})

	t.Run("empty raw type", func(t *testing.T) {
		col := &Column{Name: "id", TypeRaw: "bigint", RawType: "", RawTypeDialect: "mysql"}
		assert.False(t, col.HasTypeOverride("mysql"))
		assert.False(t, col.HasTypeOverride(""))
	})

	t.Run("override for specific dialect", func(t *testing.T) {
		col := &Column{Name: "id", TypeRaw: "bigint", RawType: "NUMBER(19)", RawTypeDialect: "oracle"}
		assert.True(t, col.HasTypeOverride("oracle"))
		assert.False(t, col.HasTypeOverride("mysql"))
		assert.True(t, col.HasTypeOverride("")) // any override exists
	})

	t.Run("whitespace only value ignored", func(t *testing.T) {
		col := &Column{Name: "id", TypeRaw: "bigint", RawType: "   ", RawTypeDialect: "oracle"}
		assert.False(t, col.HasTypeOverride("oracle"))
	})

	t.Run("case insensitive dialect lookup", func(t *testing.T) {
		col := &Column{Name: "id", TypeRaw: "bigint", RawType: "BIGSERIAL", RawTypeDialect: "postgresql"}
		assert.True(t, col.HasTypeOverride("postgresql"))
		assert.True(t, col.HasTypeOverride("PostgreSQL"))
	})
}

func TestColumnEffectiveType(t *testing.T) {
	t.Run("no override returns TypeRaw", func(t *testing.T) {
		col := &Column{Name: "id", TypeRaw: "bigint"}
		assert.Equal(t, "bigint", col.EffectiveType("mysql"))
	})

	t.Run("override takes precedence for matching dialect", func(t *testing.T) {
		col := &Column{Name: "id", TypeRaw: "bigint", RawType: "NUMBER(19)", RawTypeDialect: "oracle"}
		assert.Equal(t, "NUMBER(19)", col.EffectiveType("oracle"))
	})

	t.Run("falls back to TypeRaw for non-matching dialect", func(t *testing.T) {
		col := &Column{Name: "id", TypeRaw: "bigint", RawType: "NUMBER(19)", RawTypeDialect: "oracle"}
		assert.Equal(t, "bigint", col.EffectiveType("mysql"))
	})

	t.Run("whitespace override falls back to TypeRaw", func(t *testing.T) {
		col := &Column{Name: "id", TypeRaw: "varchar(255)", RawType: "   ", RawTypeDialect: "oracle"}
		assert.Equal(t, "varchar(255)", col.EffectiveType("oracle"))
	})

	t.Run("empty dialect returns TypeRaw", func(t *testing.T) {
		col := &Column{Name: "id", TypeRaw: "bigint", RawType: "NUMBER(19)", RawTypeDialect: "oracle"}
		assert.Equal(t, "bigint", col.EffectiveType(""))
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
		col := &Column{Name: "id", TypeRaw: "bigint", AutoIncrement: true}
		assert.False(t, col.HasIdentityOptions())
	})

	t.Run("with seed only", func(t *testing.T) {
		col := &Column{Name: "id", TypeRaw: "bigint", AutoIncrement: true, IdentitySeed: 100}
		assert.True(t, col.HasIdentityOptions())
	})

	t.Run("with increment only", func(t *testing.T) {
		col := &Column{Name: "id", TypeRaw: "bigint", AutoIncrement: true, IdentityIncrement: 5}
		assert.True(t, col.HasIdentityOptions())
	})

	t.Run("with both seed and increment", func(t *testing.T) {
		col := &Column{Name: "id", TypeRaw: "bigint", AutoIncrement: true, IdentitySeed: 1000, IdentityIncrement: 10}
		assert.True(t, col.HasIdentityOptions())
	})
}

func TestGenerationStorageConstants(t *testing.T) {
	assert.Equal(t, GenerationStorage("VIRTUAL"), GenerationVirtual)
	assert.Equal(t, GenerationStorage("STORED"), GenerationStored)
}

func TestConstraintTypeConstants(t *testing.T) {
	assert.Equal(t, ConstraintType("PRIMARY KEY"), ConstraintPrimaryKey)
	assert.Equal(t, ConstraintType("FOREIGN KEY"), ConstraintForeignKey)
	assert.Equal(t, ConstraintType("UNIQUE"), ConstraintUnique)
	assert.Equal(t, ConstraintType("CHECK"), ConstraintCheck)
}

func TestReferentialActionConstants(t *testing.T) {
	assert.Equal(t, ReferentialAction(""), RefActionNone)
	assert.Equal(t, ReferentialAction("CASCADE"), RefActionCascade)
	assert.Equal(t, ReferentialAction("RESTRICT"), RefActionRestrict)
	assert.Equal(t, ReferentialAction("SET NULL"), RefActionSetNull)
	assert.Equal(t, ReferentialAction("SET DEFAULT"), RefActionSetDefault)
	assert.Equal(t, ReferentialAction("NO ACTION"), RefActionNoAction)
}

func TestIndexTypeConstants(t *testing.T) {
	assert.Equal(t, IndexType("BTREE"), IndexTypeBTree)
	assert.Equal(t, IndexType("HASH"), IndexTypeHash)
	assert.Equal(t, IndexType("FULLTEXT"), IndexTypeFullText)
	assert.Equal(t, IndexType("SPATIAL"), IndexTypeSpatial)
	assert.Equal(t, IndexType("GIN"), IndexTypeGIN)
	assert.Equal(t, IndexType("GiST"), IndexTypeGiST)
}

func TestIndexVisibilityConstants(t *testing.T) {
	assert.Equal(t, IndexVisibility("VISIBLE"), IndexVisible)
	assert.Equal(t, IndexVisibility("INVISIBLE"), IndexInvisible)
}

func TestSortOrderConstants(t *testing.T) {
	assert.Equal(t, SortOrder("ASC"), SortAsc)
	assert.Equal(t, SortOrder("DESC"), SortDesc)
}
