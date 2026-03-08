// Note: Some options still can't be fully tested as they require specific storage engines not available in standard containers:
// - Union (MERGE tables)
// - StorageMedia / Nodegroup / TableChecksum (NDB Cluster)
// - SecondaryEngine (HeatWave - MySQL 8.0.23+ Enterprise)
package mysql

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	mysqlcontainer "github.com/testcontainers/testcontainers-go/modules/mysql"

	"smf/internal/core"
	"smf/internal/introspect"
)

func TestMySQLTableOptions(t *testing.T) {
	ctx := context.Background()

	mysqlContainer, err := mysqlcontainer.Run(ctx, "mysql:8.0")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, mysqlContainer.Terminate(ctx))
	}()

	connStr, err := mysqlContainer.ConnectionString(ctx)
	require.NoError(t, err)

	db, err := sql.Open("mysql", connStr)
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE DATABASE testdb")
	require.NoError(t, err)

	_, err = db.Exec("USE testdb")
	require.NoError(t, err)

	_, err = db.Exec(`
		CREATE TABLE users (
			id INT AUTO_INCREMENT PRIMARY KEY,
			name VARCHAR(255),
			email VARCHAR(255)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
		  ROW_FORMAT=DYNAMIC AVG_ROW_LENGTH=100
		  MAX_ROWS=1000000 MIN_ROWS=100
		  PACK_KEYS=1
		  COMMENT 'User table with various options'
	`)
	require.NoError(t, err)

	_, err = db.Exec(`
		CREATE TABLE products (
			id INT AUTO_INCREMENT PRIMARY KEY,
			name VARCHAR(255),
			price DECIMAL(10,2)
		) ENGINE=InnoDB CHARSET=utf8mb4 COLLATE=utf8mb4_bin
		  AUTO_INCREMENT=1000
		  ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8
		  COMPRESSION='ZLIB' ENCRYPTION='N'
		  STATS_PERSISTENT=1 STATS_AUTO_RECALC=DEFAULT STATS_SAMPLE_PAGES=10
		  PACK_KEYS=0 DELAY_KEY_WRITE=1
	`)
	require.NoError(t, err)

	_, err = db.Exec(`
		CREATE TABLE orders (
			id INT AUTO_INCREMENT PRIMARY KEY,
			user_id INT,
			total DECIMAL(10,2)
		) ENGINE=InnoDB
		  AUTO_INCREMENT=10000
		  CHECKSUM=1
		  DATA DIRECTORY='/var/lib/mysql-data'
		  INDEX DIRECTORY='/var/lib/mysql-index'
	`)
	require.NoError(t, err)

	_, err = db.Exec(`
		CREATE TABLE logs (
			id INT AUTO_INCREMENT PRIMARY KEY,
			message TEXT
		) ENGINE=MyISAM
		  ROW_FORMAT=FIXED
		  PACK_KEYS=DEFAULT
		  DELAY_KEY_WRITE=0
	`)
	require.NoError(t, err)

	_, err = db.Exec(`
		CREATE TABLE simple_table (
			id INT PRIMARY KEY,
			name VARCHAR(100)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
	`)
	require.NoError(t, err)

	intr, err := introspect.NewIntrospecter(core.DialectMySQL)
	require.NoError(t, err)

	_, err = db.Exec("USE testdb")
	require.NoError(t, err)

	result, err := intr.Introspect(ctx, db)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "testdb", result.Name)
	require.Equal(t, core.DialectMySQL, result.Dialect)
	require.Len(t, result.Tables, 5)

	usersTable := result.FindTable("users")
	require.NotNil(t, usersTable)
	require.NotNil(t, usersTable.Options.MySQL)
	require.Equal(t, "InnoDB", usersTable.Options.MySQL.Engine)
	require.Equal(t, "utf8mb4", usersTable.Options.MySQL.Charset)
	require.Equal(t, "utf8mb4_unicode_ci", usersTable.Options.MySQL.Collate)
	require.Equal(t, "DYNAMIC", usersTable.Options.MySQL.RowFormat)
	require.Equal(t, uint64(100), usersTable.Options.MySQL.AvgRowLength)
	require.Equal(t, uint64(1000000), usersTable.Options.MySQL.MaxRows)
	require.Equal(t, uint64(100), usersTable.Options.MySQL.MinRows)
	require.Equal(t, "1", usersTable.Options.MySQL.PackKeys)
	require.Equal(t, "User table with various options", usersTable.Comment)

	productsTable := result.FindTable("products")
	require.NotNil(t, productsTable)
	require.NotNil(t, productsTable.Options.MySQL)
	require.Equal(t, "InnoDB", productsTable.Options.MySQL.Engine)
	require.Equal(t, "utf8mb4", productsTable.Options.MySQL.Charset)
	require.Equal(t, "utf8mb4_bin", productsTable.Options.MySQL.Collate)
	require.Equal(t, uint64(1000), productsTable.Options.MySQL.AutoIncrement)
	require.Equal(t, "COMPRESSED", productsTable.Options.MySQL.RowFormat)
	require.Equal(t, uint64(8), productsTable.Options.MySQL.KeyBlockSize)
	require.Equal(t, "ZLIB", productsTable.Options.MySQL.Compression)
	require.Equal(t, "N", productsTable.Options.MySQL.Encryption)
	require.Equal(t, "1", productsTable.Options.MySQL.StatsPersistent)
	require.Equal(t, "DEFAULT", productsTable.Options.MySQL.StatsAutoRecalc)
	require.Equal(t, "10", productsTable.Options.MySQL.StatsSamplePages)
	require.Equal(t, "0", productsTable.Options.MySQL.PackKeys)
	require.Equal(t, uint64(1), productsTable.Options.MySQL.DelayKeyWrite)

	ordersTable := result.FindTable("orders")
	require.NotNil(t, ordersTable)
	require.NotNil(t, ordersTable.Options.MySQL)
	require.Equal(t, uint64(10000), ordersTable.Options.MySQL.AutoIncrement)
	require.Equal(t, uint64(1), ordersTable.Options.MySQL.Checksum)
	require.Equal(t, "/var/lib/mysql-data", ordersTable.Options.MySQL.DataDirectory)
	require.Equal(t, "/var/lib/mysql-index", ordersTable.Options.MySQL.IndexDirectory)

	logsTable := result.FindTable("logs")
	require.NotNil(t, logsTable)
	require.NotNil(t, logsTable.Options.MySQL)
	require.Equal(t, "MyISAM", logsTable.Options.MySQL.Engine)
	require.Equal(t, "FIXED", logsTable.Options.MySQL.RowFormat)
	require.Equal(t, "DEFAULT", logsTable.Options.MySQL.PackKeys)
	require.Equal(t, uint64(0), logsTable.Options.MySQL.DelayKeyWrite)

	simpleTable := result.FindTable("simple_table")
	require.NotNil(t, simpleTable)
	require.NotNil(t, simpleTable.Options.MySQL)
	require.Equal(t, "InnoDB", simpleTable.Options.MySQL.Engine)
	require.Equal(t, "utf8mb4", simpleTable.Options.MySQL.Charset)
}

func TestMySQLColumnOptions(t *testing.T) {
	ctx := context.Background()

	mysqlContainer, err := mysqlcontainer.Run(ctx, "mysql:8.0")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, mysqlContainer.Terminate(ctx))
	}()

	connStr, err := mysqlContainer.ConnectionString(ctx)
	require.NoError(t, err)

	db, err := sql.Open("mysql", connStr)
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE DATABASE testdb_col")
	require.NoError(t, err)

	_, err = db.Exec("USE testdb_col")
	require.NoError(t, err)

	_, err = db.Exec(`
		CREATE TABLE events (
			id INT PRIMARY KEY,
			name VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
			description TEXT,
			data JSON,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			is_active BOOLEAN DEFAULT TRUE,
			priority INT DEFAULT 0
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
	`)
	require.NoError(t, err)

	intr, err := introspect.NewIntrospecter(core.DialectMySQL)
	require.NoError(t, err)

	_, err = db.Exec("USE testdb_col")
	require.NoError(t, err)

	result, err := intr.Introspect(ctx, db)
	require.NoError(t, err)
	require.NotNil(t, result)

	eventsTable := result.FindTable("events")
	require.NotNil(t, eventsTable)

	idCol := eventsTable.FindColumn("id")
	require.NotNil(t, idCol)
	require.True(t, idCol.PrimaryKey)
	require.Equal(t, core.DataTypeInt, idCol.Type)

	nameCol := eventsTable.FindColumn("name")
	require.NotNil(t, nameCol)
	require.Equal(t, "utf8mb4", nameCol.Charset)
	require.Equal(t, "utf8mb4_unicode_ci", nameCol.Collate)

	createdAtCol := eventsTable.FindColumn("created_at")
	require.NotNil(t, createdAtCol)
	require.NotNil(t, createdAtCol.DefaultValue)
	require.Contains(t, *createdAtCol.DefaultValue, "CURRENT_TIMESTAMP")

	updatedAtCol := eventsTable.FindColumn("updated_at")
	require.NotNil(t, updatedAtCol)
	require.NotNil(t, updatedAtCol.OnUpdate)
	require.Contains(t, *updatedAtCol.OnUpdate, "CURRENT_TIMESTAMP")

	isActiveCol := eventsTable.FindColumn("is_active")
	require.NotNil(t, isActiveCol)
	require.Equal(t, core.DataTypeBoolean, isActiveCol.Type)
}

func TestMySQLConstraints(t *testing.T) {
	ctx := context.Background()

	mysqlContainer, err := mysqlcontainer.Run(ctx, "mysql:8.0")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, mysqlContainer.Terminate(ctx))
	}()

	connStr, err := mysqlContainer.ConnectionString(ctx)
	require.NoError(t, err)

	db, err := sql.Open("mysql", connStr)
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE DATABASE test_constraints")
	require.NoError(t, err)

	_, err = db.Exec("USE test_constraints")
	require.NoError(t, err)

	_, err = db.Exec(`
		CREATE TABLE users (
			id INT AUTO_INCREMENT PRIMARY KEY,
			email VARCHAR(255) UNIQUE NOT NULL,
			name VARCHAR(100) NOT NULL
		) ENGINE=InnoDB
	`)
	require.NoError(t, err)

	_, err = db.Exec(`
		CREATE TABLE posts (
			id INT AUTO_INCREMENT PRIMARY KEY,
			user_id INT NOT NULL,
			title VARCHAR(255) NOT NULL,
			content TEXT,
			FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE ON UPDATE RESTRICT
		) ENGINE=InnoDB
	`)
	require.NoError(t, err)

	_, err = db.Exec(`
		CREATE TABLE products (
			id INT PRIMARY KEY,
			price DECIMAL(10,2) NOT NULL,
			discount_price DECIMAL(10,2),
			CONSTRAINT chk_price CHECK (price > 0),
			CONSTRAINT chk_discount CHECK (discount_price IS NULL OR discount_price < price)
		) ENGINE=InnoDB
	`)
	require.NoError(t, err)

	intr, err := introspect.NewIntrospecter(core.DialectMySQL)
	require.NoError(t, err)

	_, err = db.Exec("USE test_constraints")
	require.NoError(t, err)

	result, err := intr.Introspect(ctx, db)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Len(t, result.Tables, 3)

	usersTable := result.FindTable("users")
	require.NotNil(t, usersTable)
	require.NotNil(t, usersTable.PrimaryKey())
	require.Len(t, usersTable.PrimaryKey().Columns, 1)
	require.Equal(t, "id", usersTable.PrimaryKey().Columns[0])

	postsTable := result.FindTable("posts")
	require.NotNil(t, postsTable)

	fkConstraint := findConstraintByType(postsTable, core.ConstraintForeignKey)
	require.NotNil(t, fkConstraint)
	require.Equal(t, "user_id", fkConstraint.Columns[0])
	require.Equal(t, "users", fkConstraint.ReferencedTable)
	require.Equal(t, "id", fkConstraint.ReferencedColumns[0])
	require.Equal(t, core.RefActionCascade, fkConstraint.OnDelete)
	require.Equal(t, core.RefActionRestrict, fkConstraint.OnUpdate)

	productsTable := result.FindTable("products")
	require.NotNil(t, productsTable)
	require.Len(t, productsTable.Constraints, 2)
}

func TestMySQLIndexes(t *testing.T) {
	ctx := context.Background()

	mysqlContainer, err := mysqlcontainer.Run(ctx, "mysql:8.0")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, mysqlContainer.Terminate(ctx))
	}()

	connStr, err := mysqlContainer.ConnectionString(ctx)
	require.NoError(t, err)

	db, err := sql.Open("mysql", connStr)
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE DATABASE test_indexes")
	require.NoError(t, err)

	_, err = db.Exec("USE test_indexes")
	require.NoError(t, err)

	_, err = db.Exec(`
		CREATE TABLE articles (
			id INT PRIMARY KEY,
			title VARCHAR(255),
			content TEXT,
			author_id INT,
			created_at DATETIME,
			FULLTEXT INDEX ft_title_content (title, content),
			INDEX idx_author (author_id, created_at DESC)
		) ENGINE=InnoDB
	`)
	require.NoError(t, err)

	_, err = db.Exec(`
		CREATE TABLE locations (
			id INT PRIMARY KEY,
			name VARCHAR(100),
			lat DECIMAL(10,8),
			lng DECIMAL(11,8),
			SPATIAL INDEX sp_locations (lat, lng)
		) ENGINE=InnoDB
	`)
	require.NoError(t, err)

	intr, err := introspect.NewIntrospecter(core.DialectMySQL)
	require.NoError(t, err)

	_, err = db.Exec("USE test_indexes")
	require.NoError(t, err)

	result, err := intr.Introspect(ctx, db)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Len(t, result.Tables, 2)

	articlesTable := result.FindTable("articles")
	require.NotNil(t, articlesTable)
	require.NotNil(t, articlesTable.Indexes)
	require.GreaterOrEqual(t, len(articlesTable.Indexes), 2)

	locationsTable := result.FindTable("locations")
	require.NotNil(t, locationsTable)
}

func TestMySQLAllTableOptions(t *testing.T) {
	ctx := context.Background()

	mysqlContainer, err := mysqlcontainer.Run(ctx, "mysql:8.0")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, mysqlContainer.Terminate(ctx))
	}()

	connStr, err := mysqlContainer.ConnectionString(ctx)
	require.NoError(t, err)

	db, err := sql.Open("mysql", connStr)
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE DATABASE test_all_options")
	require.NoError(t, err)

	_, err = db.Exec("USE test_all_options")
	require.NoError(t, err)

	testCases := []struct {
		name   string
		schema string
		verify func(*testing.T, *core.Database)
	}{
		{
			name: "InnoDB with compression and encryption",
			schema: `
				CREATE TABLE t_innodb_compressed (
					id INT PRIMARY KEY,
					data VARCHAR(255)
				) ENGINE=InnoDB
				  ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8
				  COMPRESSION='ZLIB' ENCRYPTION='Y'
			`,
			verify: func(t *testing.T, db *core.Database) {
				t.Helper()
				tbl := db.FindTable("t_innodb_compressed")
				require.NotNil(t, tbl)
				opts := tbl.Options.MySQL
				require.Equal(t, "InnoDB", opts.Engine)
				require.Equal(t, "COMPRESSED", opts.RowFormat)
				require.Equal(t, uint64(8), opts.KeyBlockSize)
				require.Equal(t, "ZLIB", opts.Compression)
				require.Equal(t, "Y", opts.Encryption)
			},
		},
		{
			name: "InnoDB with statistics options",
			schema: `
				CREATE TABLE t_innodb_stats (
					id INT PRIMARY KEY,
					data VARCHAR(255)
				) ENGINE=InnoDB
				  STATS_PERSISTENT=1
				  STATS_AUTO_RECALC=0
				  STATS_SAMPLE_PAGES=20
			`,
			verify: func(t *testing.T, db *core.Database) {
				t.Helper()
				tbl := db.FindTable("t_innodb_stats")
				require.NotNil(t, tbl)
				opts := tbl.Options.MySQL
				require.Equal(t, "1", opts.StatsPersistent)
				require.Equal(t, "0", opts.StatsAutoRecalc)
				require.Equal(t, "20", opts.StatsSamplePages)
			},
		},
		{
			name: "MyISAM with pack keys and delay key write",
			schema: `
				CREATE TABLE t_myisam (
					id INT PRIMARY KEY,
					data VARCHAR(255)
				) ENGINE=MyISAM
				  ROW_FORMAT=Dynamic
				  PACK_KEYS=1
				  DELAY_KEY_WRITE=1
			`,
			verify: func(t *testing.T, db *core.Database) {
				t.Helper()
				tbl := db.FindTable("t_myisam")
				require.NotNil(t, tbl)
				opts := tbl.Options.MySQL
				require.Equal(t, "MyISAM", opts.Engine)
				require.Equal(t, "Dynamic", opts.RowFormat)
				require.Equal(t, "1", opts.PackKeys)
				require.Equal(t, uint64(1), opts.DelayKeyWrite)
			},
		},
		{
			name: "InnoDB with auto increment and row format",
			schema: `
				CREATE TABLE t_auto_increment (
					id BIGINT PRIMARY KEY,
					data VARCHAR(255)
				) ENGINE=InnoDB
				  AUTO_INCREMENT=100000
				  ROW_FORMAT=Compact
			`,
			verify: func(t *testing.T, db *core.Database) {
				t.Helper()
				tbl := db.FindTable("t_auto_increment")
				require.NotNil(t, tbl)
				opts := tbl.Options.MySQL
				require.Equal(t, uint64(100000), opts.AutoIncrement)
				require.Equal(t, "Compact", opts.RowFormat)
			},
		},
		{
			name: "InnoDB with charset and collation",
			schema: `
				CREATE TABLE t_charset (
					id INT PRIMARY KEY,
					data VARCHAR(255)
				) ENGINE=InnoDB
				  CHARSET=utf8mb4
				  COLLATE=utf8mb4_unicode_ci
			`,
			verify: func(t *testing.T, db *core.Database) {
				t.Helper()
				tbl := db.FindTable("t_charset")
				require.NotNil(t, tbl)
				opts := tbl.Options.MySQL
				require.Equal(t, "utf8mb4", opts.Charset)
				require.Equal(t, "utf8mb4_unicode_ci", opts.Collate)
			},
		},
		{
			name: "Table with all hints",
			schema: `
				CREATE TABLE t_hints (
					id INT PRIMARY KEY,
					data VARCHAR(255)
				) ENGINE=InnoDB
				  AVG_ROW_LENGTH=128
				  MAX_ROWS=500000
				  MIN_ROWS=10
			`,
			verify: func(t *testing.T, db *core.Database) {
				t.Helper()
				tbl := db.FindTable("t_hints")
				require.NotNil(t, tbl)
				opts := tbl.Options.MySQL
				require.Equal(t, uint64(128), opts.AvgRowLength)
				require.Equal(t, uint64(500000), opts.MaxRows)
				require.Equal(t, uint64(10), opts.MinRows)
			},
		},
		{
			name: "Table with checksum",
			schema: `
				CREATE TABLE t_checksum (
					id INT PRIMARY KEY,
					data VARCHAR(255)
				) ENGINE=InnoDB
				  CHECKSUM=1
			`,
			verify: func(t *testing.T, db *core.Database) {
				t.Helper()
				tbl := db.FindTable("t_checksum")
				require.NotNil(t, tbl)
				opts := tbl.Options.MySQL
				require.Equal(t, uint64(1), opts.Checksum)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := db.Exec(tc.schema)
			require.NoError(t, err)
		})
	}

	intr, err := introspect.NewIntrospecter(core.DialectMySQL)
	require.NoError(t, err)

	_, err = db.Exec("USE test_all_options")
	require.NoError(t, err)

	result, err := intr.Introspect(ctx, db)
	require.NoError(t, err)
	require.NotNil(t, result)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.verify(t, result)
		})
	}
}

func TestMySQLGeneratedColumns(t *testing.T) {
	ctx := context.Background()

	mysqlContainer, err := mysqlcontainer.Run(ctx, "mysql:8.0")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, mysqlContainer.Terminate(ctx))
	}()

	connStr, err := mysqlContainer.ConnectionString(ctx)
	require.NoError(t, err)

	db, err := sql.Open("mysql", connStr)
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE DATABASE test_generated")
	require.NoError(t, err)

	_, err = db.Exec("USE test_generated")
	require.NoError(t, err)

	_, err = db.Exec(`
		CREATE TABLE orders (
			id INT PRIMARY KEY,
			quantity INT NOT NULL,
			unit_price DECIMAL(10,2) NOT NULL,
			total_price DECIMAL(10,2) AS (quantity * unit_price) STORED,
			total_price_virtual DECIMAL(10,2) AS (quantity * unit_price) VIRTUAL
		) ENGINE=InnoDB
	`)
	require.NoError(t, err)

	intr, err := introspect.NewIntrospecter(core.DialectMySQL)
	require.NoError(t, err)

	_, err = db.Exec("USE test_generated")
	require.NoError(t, err)

	result, err := intr.Introspect(ctx, db)
	require.NoError(t, err)
	require.NotNil(t, result)

	ordersTable := result.FindTable("orders")
	require.NotNil(t, ordersTable)

	totalPriceCol := ordersTable.FindColumn("total_price")
	require.NotNil(t, totalPriceCol)
	require.True(t, totalPriceCol.IsGenerated)
	require.Equal(t, "(quantity * unit_price)", totalPriceCol.GenerationExpression)
	require.Equal(t, core.GenerationStored, totalPriceCol.GenerationStorage)

	totalPriceVirtualCol := ordersTable.FindColumn("total_price_virtual")
	require.NotNil(t, totalPriceVirtualCol)
	require.True(t, totalPriceVirtualCol.IsGenerated)
	require.Equal(t, core.GenerationVirtual, totalPriceVirtualCol.GenerationStorage)
}

func TestMySQLEnumAndSet(t *testing.T) {
	ctx := context.Background()

	mysqlContainer, err := mysqlcontainer.Run(ctx, "mysql:8.0")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, mysqlContainer.Terminate(ctx))
	}()

	connStr, err := mysqlContainer.ConnectionString(ctx)
	require.NoError(t, err)

	db, err := sql.Open("mysql", connStr)
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE DATABASE test_enum")
	require.NoError(t, err)

	_, err = db.Exec("USE test_enum")
	require.NoError(t, err)

	_, err = db.Exec(`
		CREATE TABLE subscriptions (
			id INT PRIMARY KEY,
			status ENUM('active', 'inactive', 'pending', 'cancelled') NOT NULL,
			tier SET('free', 'basic', 'premium', 'enterprise') DEFAULT 'free'
		) ENGINE=InnoDB
	`)
	require.NoError(t, err)

	intr, err := introspect.NewIntrospecter(core.DialectMySQL)
	require.NoError(t, err)

	_, err = db.Exec("USE test_enum")
	require.NoError(t, err)

	result, err := intr.Introspect(ctx, db)
	require.NoError(t, err)
	require.NotNil(t, result)

	subsTable := result.FindTable("subscriptions")
	require.NotNil(t, subsTable)

	statusCol := subsTable.FindColumn("status")
	require.NotNil(t, statusCol)
	require.Equal(t, core.DataTypeEnum, statusCol.Type)
	require.Equal(t, []string{"active", "inactive", "pending", "canceled"}, statusCol.EnumValues)
	require.False(t, statusCol.Nullable)

	tierCol := subsTable.FindColumn("tier")
	require.NotNil(t, tierCol)
	require.Equal(t, core.DataTypeEnum, tierCol.Type)
	require.Contains(t, tierCol.EnumValues, "free")
	require.Contains(t, tierCol.EnumValues, "basic")
	require.Contains(t, tierCol.EnumValues, "premium")
	require.Contains(t, tierCol.EnumValues, "enterprise")
}

func TestMySQLVersionDetection(t *testing.T) {
	ctx := context.Background()

	testVersions := []string{"8.0", "8.0.23", "8.0.35"}

	for _, version := range testVersions {
		t.Run("MySQL_"+version, func(t *testing.T) {
			if version == "8.0" {
				t.Skip("Skipping generic 8.0 tag, use specific version")
				return
			}

			mysqlContainer, err := mysqlcontainer.Run(ctx, "mysql:"+version)
			require.NoError(t, err)
			defer func() {
				require.NoError(t, mysqlContainer.Terminate(ctx))
			}()

			connStr, err := mysqlContainer.ConnectionString(ctx)
			require.NoError(t, err)

			db, err := sql.Open("mysql", connStr)
			require.NoError(t, err)
			defer db.Close()

			_, err = db.Exec("CREATE DATABASE test_version")
			require.NoError(t, err)

			_, err = db.Exec("USE test_version")
			require.NoError(t, err)

			_, err = db.Exec(`
				CREATE TABLE t1 (id INT PRIMARY KEY) ENGINE=InnoDB
			`)
			require.NoError(t, err)

			intr, err := introspect.NewIntrospecter(core.DialectMySQL)
			require.NoError(t, err)

			_, err = db.Exec("USE test_version")
			require.NoError(t, err)

			result, err := intr.Introspect(ctx, db)
			require.NoError(t, err)
			require.NotNil(t, result)
			require.Equal(t, core.DialectMySQL, result.Dialect)
		})
	}
}

func TestMySQLInvisibleColumn(t *testing.T) {
	ctx := context.Background()

	mysqlContainer, err := mysqlcontainer.Run(ctx, "mysql:8.0")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, mysqlContainer.Terminate(ctx))
	}()

	connStr, err := mysqlContainer.ConnectionString(ctx)
	require.NoError(t, err)

	db, err := sql.Open("mysql", connStr)
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE DATABASE test_invisible")
	require.NoError(t, err)

	_, err = db.Exec("USE test_invisible")
	require.NoError(t, err)

	_, err = db.Exec(`
		CREATE TABLE users (
			id INT PRIMARY KEY,
			name VARCHAR(255),
			secret_data VARCHAR(255) INVISIBLE
		) ENGINE=InnoDB
	`)
	require.NoError(t, err)

	intr, err := introspect.NewIntrospecter(core.DialectMySQL)
	require.NoError(t, err)

	_, err = db.Exec("USE test_invisible")
	require.NoError(t, err)

	result, err := intr.Introspect(ctx, db)
	require.NoError(t, err)
	require.NotNil(t, result)

	usersTable := result.FindTable("users")
	require.NotNil(t, usersTable)

	idCol := usersTable.FindColumn("id")
	require.NotNil(t, idCol)
	require.False(t, idCol.Invisible)

	nameCol := usersTable.FindColumn("name")
	require.NotNil(t, nameCol)
	require.False(t, nameCol.Invisible)

	secretCol := usersTable.FindColumn("secret_data")
	require.NotNil(t, secretCol)
	require.True(t, secretCol.Invisible)
}

func TestMySQLInvisibleIndex(t *testing.T) {
	ctx := context.Background()

	mysqlContainer, err := mysqlcontainer.Run(ctx, "mysql:8.0")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, mysqlContainer.Terminate(ctx))
	}()

	connStr, err := mysqlContainer.ConnectionString(ctx)
	require.NoError(t, err)

	db, err := sql.Open("mysql", connStr)
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE DATABASE test_inv_idx")
	require.NoError(t, err)

	_, err = db.Exec("USE test_inv_idx")
	require.NoError(t, err)

	_, err = db.Exec(`
		CREATE TABLE products (
			id INT PRIMARY KEY,
			name VARCHAR(255),
			sku VARCHAR(50),
			INDEX idx_sku (sku) VISIBLE,
			INDEX idx_name (name) INVISIBLE
		) ENGINE=InnoDB
	`)
	require.NoError(t, err)

	intr, err := introspect.NewIntrospecter(core.DialectMySQL)
	require.NoError(t, err)

	_, err = db.Exec("USE test_inv_idx")
	require.NoError(t, err)

	result, err := intr.Introspect(ctx, db)
	require.NoError(t, err)
	require.NotNil(t, result)

	productsTable := result.FindTable("products")
	require.NotNil(t, productsTable)

	visibleIdx := productsTable.FindIndex("idx_sku")
	require.NotNil(t, visibleIdx)
	require.Equal(t, core.IndexVisible, visibleIdx.Visibility)

	invisibleIdx := productsTable.FindIndex("idx_name")
	require.NotNil(t, invisibleIdx)
	require.Equal(t, core.IndexInvisible, invisibleIdx.Visibility)
}

func TestMySQLEngineAttributes(t *testing.T) {
	ctx := context.Background()

	mysqlContainer, err := mysqlcontainer.Run(ctx, "mysql:8.0")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, mysqlContainer.Terminate(ctx))
	}()

	connStr, err := mysqlContainer.ConnectionString(ctx)
	require.NoError(t, err)

	db, err := sql.Open("mysql", connStr)
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE DATABASE test_engine_attr")
	require.NoError(t, err)

	_, err = db.Exec("USE test_engine_attr")
	require.NoError(t, err)

	_, err = db.Exec(`
		CREATE TABLE t1 (
			id INT PRIMARY KEY,
			data VARCHAR(255)
		) ENGINE=InnoDB
		  ENGINE_ATTRIBUTE='{"primary": true, "zone": "us-east-1"}'
		  SECONDARY_ENGINE_ATTRIBUTE='{"offload": true}'
	`)
	require.NoError(t, err)

	intr, err := introspect.NewIntrospecter(core.DialectMySQL)
	require.NoError(t, err)

	_, err = db.Exec("USE test_engine_attr")
	require.NoError(t, err)

	result, err := intr.Introspect(ctx, db)
	require.NoError(t, err)
	require.NotNil(t, result)

	tbl := result.FindTable("t1")
	require.NotNil(t, tbl)
	require.NotNil(t, tbl.Options.MySQL)
	require.JSONEq(t, `{"primary": true, "zone": "us-east-1"}`, tbl.Options.MySQL.EngineAttribute)
	require.Equal(t, `{"offload": true}`, tbl.Options.MySQL.SecondaryEngineAttribute)
}

func TestMySQLPageCompression(t *testing.T) {
	ctx := context.Background()

	mysqlContainer, err := mysqlcontainer.Run(ctx, "mysql:8.0")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, mysqlContainer.Terminate(ctx))
	}()

	connStr, err := mysqlContainer.ConnectionString(ctx)
	require.NoError(t, err)

	db, err := sql.Open("mysql", connStr)
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE DATABASE test_page_compress")
	require.NoError(t, err)

	_, err = db.Exec("USE test_page_compress")
	require.NoError(t, err)

	_, err = db.Exec(`
		CREATE TABLE t1 (
			id INT PRIMARY KEY,
			data VARCHAR(255)
		) ENGINE=InnoDB
		  PAGE_COMPRESSED=1
		  PAGE_COMPRESSION_LEVEL=9
	`)
	require.NoError(t, err)

	_, err = db.Exec(`
		CREATE TABLE t2 (
			id INT PRIMARY KEY,
			data VARCHAR(255)
		) ENGINE=InnoDB
		  PAGE_COMPRESSED=0
	`)
	require.NoError(t, err)

	intr, err := introspect.NewIntrospecter(core.DialectMySQL)
	require.NoError(t, err)

	_, err = db.Exec("USE test_page_compress")
	require.NoError(t, err)

	result, err := intr.Introspect(ctx, db)
	require.NoError(t, err)
	require.NotNil(t, result)

	tbl1 := result.FindTable("t1")
	require.NotNil(t, tbl1)
	require.NotNil(t, tbl1.Options.MySQL)
	require.True(t, tbl1.Options.MySQL.PageCompressed)
	require.Equal(t, uint64(9), tbl1.Options.MySQL.PageCompressionLevel)

	tbl2 := result.FindTable("t2")
	require.NotNil(t, tbl2)
	require.NotNil(t, tbl2.Options.MySQL)
	require.False(t, tbl2.Options.MySQL.PageCompressed)
}

func TestMySQLFederatedOptions(t *testing.T) {
	ctx := context.Background()

	mysqlContainer, err := mysqlcontainer.Run(ctx, "mysql:8.0")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, mysqlContainer.Terminate(ctx))
	}()

	connStr, err := mysqlContainer.ConnectionString(ctx)
	require.NoError(t, err)

	db, err := sql.Open("mysql", connStr)
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE DATABASE test_federated")
	require.NoError(t, err)

	_, err = db.Exec("USE test_federated")
	require.NoError(t, err)

	_, err = db.Exec(`
		CREATE TABLE remote_users (
			id INT PRIMARY KEY,
			name VARCHAR(255)
		) ENGINE=InnoDB
	`)
	require.NoError(t, err)

	_, err = db.Exec(`
		CREATE TABLE federated_table (
			id INT PRIMARY KEY,
			user_id INT,
			data VARCHAR(255)
		) ENGINE=FEDERATED
		  CONNECTION='mysql://user:password@remote_host:3306/testdb/remote_users'
		  PASSWORD='password'
		  COMMENT 'Federated table'
	`)
	require.NoError(t, err)

	intr, err := introspect.NewIntrospecter(core.DialectMySQL)
	require.NoError(t, err)

	_, err = db.Exec("USE test_federated")
	require.NoError(t, err)

	result, err := intr.Introspect(ctx, db)
	require.NoError(t, err)
	require.NotNil(t, result)

	federatedTable := result.FindTable("federated_table")
	require.NotNil(t, federatedTable)
	require.NotNil(t, federatedTable.Options.MySQL)
	require.Equal(t, "FEDERATED", federatedTable.Options.MySQL.Engine)
	require.Equal(t, "mysql://user:password@remote_host:3306/testdb/remote_users", federatedTable.Options.MySQL.Connection)
	require.Equal(t, "password", federatedTable.Options.MySQL.Password)
	require.Equal(t, "Federated table", federatedTable.Comment)
}

func TestMySQLColumnEngineAttributes(t *testing.T) {
	ctx := context.Background()

	mysqlContainer, err := mysqlcontainer.Run(ctx, "mysql:8.0")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, mysqlContainer.Terminate(ctx))
	}()

	connStr, err := mysqlContainer.ConnectionString(ctx)
	require.NoError(t, err)

	db, err := sql.Open("mysql", connStr)
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE DATABASE test_col_engine_attr")
	require.NoError(t, err)

	_, err = db.Exec("USE test_col_engine_attr")
	require.NoError(t, err)

	_, err = db.Exec(`
		CREATE TABLE t1 (
			id INT PRIMARY KEY,
			data VARCHAR(255) PRIMARY_KEY_ENGINE_ATTRIBUTE='{"index": "primary"}'
		) ENGINE=InnoDB
	`)
	require.NoError(t, err)

	_, err = db.Exec(`
		CREATE TABLE t2 (
			id INT PRIMARY KEY,
			data VARCHAR(255) SECONDARY_ENGINE_ATTRIBUTE='{"rapidenabled": true}'
		) ENGINE=InnoDB
	`)
	require.NoError(t, err)

	intr, err := introspect.NewIntrospecter(core.DialectMySQL)
	require.NoError(t, err)

	_, err = db.Exec("USE test_col_engine_attr")
	require.NoError(t, err)

	result, err := intr.Introspect(ctx, db)
	require.NoError(t, err)
	require.NotNil(t, result)

	tbl1 := result.FindTable("t1")
	require.NotNil(t, tbl1)
	dataCol1 := tbl1.FindColumn("data")
	require.NotNil(t, dataCol1)
	require.NotNil(t, dataCol1.MySQL)
	require.JSONEq(t, `{"index": "primary"}`, dataCol1.MySQL.PrimaryEngineAttribute)

	tbl2 := result.FindTable("t2")
	require.NotNil(t, tbl2)
	dataCol2 := tbl2.FindColumn("data")
	require.NotNil(t, dataCol2)
	require.NotNil(t, dataCol2.MySQL)
	require.Equal(t, `{"rapidenabled": true}`, dataCol2.MySQL.SecondaryEngineAttribute)
}

func TestMySQLInsertMethod(t *testing.T) {
	ctx := context.Background()

	mysqlContainer, err := mysqlcontainer.Run(ctx, "mysql:8.0")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, mysqlContainer.Terminate(ctx))
	}()

	connStr, err := mysqlContainer.ConnectionString(ctx)
	require.NoError(t, err)

	db, err := sql.Open("mysql", connStr)
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE DATABASE test_insert_method")
	require.NoError(t, err)

	_, err = db.Exec("USE test_insert_method")
	require.NoError(t, err)

	_, err = db.Exec(`
		CREATE TABLE t1 (
			id INT PRIMARY KEY,
			data VARCHAR(255)
		) ENGINE=MyISAM
		  INSERT_METHOD=NO
	`)
	require.NoError(t, err)

	_, err = db.Exec(`
		CREATE TABLE t2 (
			id INT PRIMARY KEY,
			data VARCHAR(255)
		) ENGINE=MyISAM
		  INSERT_METHOD=FIRST
	`)
	require.NoError(t, err)

	_, err = db.Exec(`
		CREATE TABLE t3 (
			id INT PRIMARY KEY,
			data VARCHAR(255)
		) ENGINE=MyISAM
		  INSERT_METHOD=LAST
	`)
	require.NoError(t, err)

	intr, err := introspect.NewIntrospecter(core.DialectMySQL)
	require.NoError(t, err)

	_, err = db.Exec("USE test_insert_method")
	require.NoError(t, err)

	result, err := intr.Introspect(ctx, db)
	require.NoError(t, err)
	require.NotNil(t, result)

	tbl1 := result.FindTable("t1")
	require.NotNil(t, tbl1)
	require.NotNil(t, tbl1.Options.MySQL)
	require.Equal(t, "NO", tbl1.Options.MySQL.InsertMethod)

	tbl2 := result.FindTable("t2")
	require.NotNil(t, tbl2)
	require.NotNil(t, tbl2.Options.MySQL)
	require.Equal(t, "FIRST", tbl2.Options.MySQL.InsertMethod)

	tbl3 := result.FindTable("t3")
	require.NotNil(t, tbl3)
	require.NotNil(t, tbl3.Options.MySQL)
	require.Equal(t, "LAST", tbl3.Options.MySQL.InsertMethod)
}

func TestMySQLIETFQuotes(t *testing.T) {
	ctx := context.Background()

	mysqlContainer, err := mysqlcontainer.Run(ctx, "mysql:8.0")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, mysqlContainer.Terminate(ctx))
	}()

	connStr, err := mysqlContainer.ConnectionString(ctx)
	require.NoError(t, err)

	db, err := sql.Open("mysql", connStr)
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE DATABASE test_ietf")
	require.NoError(t, err)

	_, err = db.Exec("USE test_ietf")
	require.NoError(t, err)

	_, err = db.Exec(`
		CREATE TABLE t1 (
			id INT PRIMARY KEY,
			data VARCHAR(255)
		) ENGINE=CSV
		  IETF_QUOTES=1
	`)
	require.NoError(t, err)

	_, err = db.Exec(`
		CREATE TABLE t2 (
			id INT PRIMARY KEY,
			data VARCHAR(255)
		) ENGINE=CSV
		  IETF_QUOTES=0
	`)
	require.NoError(t, err)

	intr, err := introspect.NewIntrospecter(core.DialectMySQL)
	require.NoError(t, err)

	_, err = db.Exec("USE test_ietf")
	require.NoError(t, err)

	result, err := intr.Introspect(ctx, db)
	require.NoError(t, err)
	require.NotNil(t, result)

	tbl1 := result.FindTable("t1")
	require.NotNil(t, tbl1)
	require.NotNil(t, tbl1.Options.MySQL)
	require.True(t, tbl1.Options.MySQL.IETFQuotes)

	tbl2 := result.FindTable("t2")
	require.NotNil(t, tbl2)
	require.NotNil(t, tbl2.Options.MySQL)
	require.False(t, tbl2.Options.MySQL.IETFQuotes)
}

func TestMySQLAutoextendSize(t *testing.T) {
	ctx := context.Background()

	mysqlContainer, err := mysqlcontainer.Run(ctx, "mysql:8.0")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, mysqlContainer.Terminate(ctx))
	}()

	connStr, err := mysqlContainer.ConnectionString(ctx)
	require.NoError(t, err)

	db, err := sql.Open("mysql", connStr)
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE DATABASE test_autoextend")
	require.NoError(t, err)

	_, err = db.Exec("USE test_autoextend")
	require.NoError(t, err)

	_, err = db.Exec(`
		CREATE TABLE t1 (
			id INT PRIMARY KEY,
			data VARCHAR(255)
		) ENGINE=InnoDB
		  AUTOEXTEND_SIZE=134217728
	`)
	require.NoError(t, err)

	intr, err := introspect.NewIntrospecter(core.DialectMySQL)
	require.NoError(t, err)

	_, err = db.Exec("USE test_autoextend")
	require.NoError(t, err)

	result, err := intr.Introspect(ctx, db)
	require.NoError(t, err)
	require.NotNil(t, result)

	tbl := result.FindTable("t1")
	require.NotNil(t, tbl)
	require.NotNil(t, tbl.Options.MySQL)
	require.Equal(t, "134217728", tbl.Options.MySQL.AutoextendSize)
}
