package mysql

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	mariadbcontainer "github.com/testcontainers/testcontainers-go/modules/mariadb"

	"smf/internal/core"
	"smf/internal/introspect"
)

func TestMariaDBCoreIntrospectionScenarios(t *testing.T) {
	ctx := context.Background()
	mariaDBContainer, err := mariadbcontainer.Run(ctx, "mariadb:11.0.3")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, mariaDBContainer.Terminate(ctx))
	}()

	connStr, err := mariaDBContainer.ConnectionString(ctx)
	require.NoError(t, err)

	db, err := sql.Open("mysql", connStr)
	require.NoError(t, err)
	defer db.Close()

	intr, err := introspect.NewIntrospecter(core.DialectMariaDB)
	require.NoError(t, err)

	type scenario struct {
		name     string
		database string
		schema   []string
		verify   func(*testing.T, *core.Database)
	}

	scenarios := []scenario{
		{
			name:     "Table options",
			database: "testdb",
			schema: []string{
				`CREATE TABLE users (
					id INT AUTO_INCREMENT PRIMARY KEY,
					name VARCHAR(255),
					email VARCHAR(255)
				) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
				  ROW_FORMAT=DYNAMIC AVG_ROW_LENGTH=100
				  MAX_ROWS=1000000 MIN_ROWS=100
				  PACK_KEYS=1
				  COMMENT 'User table with various options'`,
				`CREATE TABLE products (
					id INT AUTO_INCREMENT PRIMARY KEY,
					name VARCHAR(255),
					price DECIMAL(10,2)
				) ENGINE=InnoDB CHARSET=utf8mb4 COLLATE=utf8mb4_bin
				  AUTO_INCREMENT=1000
				  ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8
				  COMPRESSION='ZLIB' ENCRYPTION='N'
				  STATS_PERSISTENT=1 STATS_AUTO_RECALC=DEFAULT STATS_SAMPLE_PAGES=10
				  PACK_KEYS=0 DELAY_KEY_WRITE=1`,
				`CREATE TABLE orders (
					id INT AUTO_INCREMENT PRIMARY KEY,
					user_id INT,
					total DECIMAL(10,2)
				) ENGINE=InnoDB
				  AUTO_INCREMENT=10000
				  CHECKSUM=1
				  DATA DIRECTORY='/var/lib/mysql-data'
				  INDEX DIRECTORY='/var/lib/mysql-index'`,
				`CREATE TABLE logs (
					id INT AUTO_INCREMENT PRIMARY KEY,
					message TEXT
				) ENGINE=MyISAM
				  ROW_FORMAT=FIXED
				  PACK_KEYS=DEFAULT
				  DELAY_KEY_WRITE=0`,
				`CREATE TABLE simple_table (
					id INT PRIMARY KEY,
					name VARCHAR(100)
				) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
			},
			verify: func(t *testing.T, result *core.Database) {
				t.Helper()
				require.Equal(t, "testdb", result.Name)
				require.Equal(t, core.DialectMariaDB, result.Dialect)
				require.Len(t, result.Tables, 5)

				usersTable := result.FindTable("users")
				require.NotNil(t, usersTable)
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
				require.Equal(t, uint64(10000), ordersTable.Options.MySQL.AutoIncrement)
				require.Equal(t, uint64(1), ordersTable.Options.MySQL.Checksum)
				require.Equal(t, "/var/lib/mysql-data", ordersTable.Options.MySQL.DataDirectory)
				require.Equal(t, "/var/lib/mysql-index", ordersTable.Options.MySQL.IndexDirectory)

				logsTable := result.FindTable("logs")
				require.NotNil(t, logsTable)
				require.Equal(t, "MyISAM", logsTable.Options.MySQL.Engine)
				require.Equal(t, "FIXED", logsTable.Options.MySQL.RowFormat)
				require.Equal(t, "DEFAULT", logsTable.Options.MySQL.PackKeys)
				require.Equal(t, uint64(0), logsTable.Options.MySQL.DelayKeyWrite)

				simpleTable := result.FindTable("simple_table")
				require.NotNil(t, simpleTable)
				require.Equal(t, "InnoDB", simpleTable.Options.MySQL.Engine)
				require.Equal(t, "utf8mb4", simpleTable.Options.MySQL.Charset)
			},
		},
		{
			name:     "Column options",
			database: "testdb_col",
			schema: []string{`CREATE TABLE events (
				id INT PRIMARY KEY,
				name VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
				description TEXT,
				data JSON,
				created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
				is_active BOOLEAN DEFAULT TRUE,
				priority INT DEFAULT 0
			) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`},
			verify: func(t *testing.T, result *core.Database) {
				t.Helper()
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
			},
		},
		{
			name:     "Constraints and indexes",
			database: "test_constraints_idx",
			schema: []string{
				`CREATE TABLE users (
					id INT AUTO_INCREMENT PRIMARY KEY,
					email VARCHAR(255) UNIQUE NOT NULL,
					name VARCHAR(100) NOT NULL
				) ENGINE=InnoDB`,
				`CREATE TABLE posts (
					id INT AUTO_INCREMENT PRIMARY KEY,
					user_id INT NOT NULL,
					title VARCHAR(255) NOT NULL,
					content TEXT,
					FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE ON UPDATE RESTRICT
				) ENGINE=InnoDB`,
				`CREATE TABLE products (
					id INT PRIMARY KEY,
					price DECIMAL(10,2) NOT NULL,
					discount_price DECIMAL(10,2),
					CONSTRAINT chk_price CHECK (price > 0),
					CONSTRAINT chk_discount CHECK (discount_price IS NULL OR discount_price < price)
				) ENGINE=InnoDB`,
				`CREATE TABLE articles (
					id INT PRIMARY KEY,
					title VARCHAR(255),
					content TEXT,
					author_id INT,
					created_at DATETIME,
					FULLTEXT INDEX ft_title_content (title, content),
					INDEX idx_author (author_id, created_at DESC)
				) ENGINE=InnoDB`,
				`CREATE TABLE locations (
					id INT PRIMARY KEY,
					name VARCHAR(100),
					lat DECIMAL(10,8),
					lng DECIMAL(11,8),
					SPATIAL INDEX sp_locations (lat, lng)
				) ENGINE=InnoDB`,
			},
			verify: func(t *testing.T, result *core.Database) {
				t.Helper()
				usersTable := result.FindTable("users")
				require.NotNil(t, usersTable)
				require.NotNil(t, usersTable.PrimaryKey())
				require.Equal(t, "id", usersTable.PrimaryKey().Columns[0])
				postsTable := result.FindTable("posts")
				require.NotNil(t, postsTable)
				fkConstraint := findConstraintByType(postsTable, core.ConstraintForeignKey)
				require.NotNil(t, fkConstraint)
				require.Equal(t, "users", fkConstraint.ReferencedTable)
				require.Equal(t, core.RefActionCascade, fkConstraint.OnDelete)
				require.Equal(t, core.RefActionRestrict, fkConstraint.OnUpdate)
				productsTable := result.FindTable("products")
				require.NotNil(t, productsTable)
				require.Len(t, productsTable.Constraints, 2)
				articlesTable := result.FindTable("articles")
				require.NotNil(t, articlesTable)
				require.GreaterOrEqual(t, len(articlesTable.Indexes), 2)
				locationsTable := result.FindTable("locations")
				require.NotNil(t, locationsTable)
			},
		},
		{
			name:     "All table options",
			database: "test_all_options",
			schema: []string{
				`CREATE TABLE t_innodb_compressed (id INT PRIMARY KEY, data VARCHAR(255)) ENGINE=InnoDB ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8 COMPRESSION='ZLIB' ENCRYPTION='Y'`,
				`CREATE TABLE t_innodb_stats (id INT PRIMARY KEY, data VARCHAR(255)) ENGINE=InnoDB STATS_PERSISTENT=1 STATS_AUTO_RECALC=0 STATS_SAMPLE_PAGES=20`,
				`CREATE TABLE t_myisam (id INT PRIMARY KEY, data VARCHAR(255)) ENGINE=MyISAM ROW_FORMAT=Dynamic PACK_KEYS=1 DELAY_KEY_WRITE=1`,
				`CREATE TABLE t_auto_increment (id BIGINT PRIMARY KEY, data VARCHAR(255)) ENGINE=InnoDB AUTO_INCREMENT=100000 ROW_FORMAT=Compact`,
				`CREATE TABLE t_charset (id INT PRIMARY KEY, data VARCHAR(255)) ENGINE=InnoDB CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci`,
				`CREATE TABLE t_hints (id INT PRIMARY KEY, data VARCHAR(255)) ENGINE=InnoDB AVG_ROW_LENGTH=128 MAX_ROWS=500000 MIN_ROWS=10`,
				`CREATE TABLE t_checksum (id INT PRIMARY KEY, data VARCHAR(255)) ENGINE=InnoDB CHECKSUM=1`,
			},
			verify: func(t *testing.T, result *core.Database) {
				t.Helper()
				require.Equal(t, "Y", result.FindTable("t_innodb_compressed").Options.MySQL.Encryption)
				require.Equal(t, "20", result.FindTable("t_innodb_stats").Options.MySQL.StatsSamplePages)
				require.Equal(t, "1", result.FindTable("t_myisam").Options.MySQL.PackKeys)
				require.Equal(t, uint64(100000), result.FindTable("t_auto_increment").Options.MySQL.AutoIncrement)
				require.Equal(t, "utf8mb4_unicode_ci", result.FindTable("t_charset").Options.MySQL.Collate)
				require.Equal(t, uint64(500000), result.FindTable("t_hints").Options.MySQL.MaxRows)
				require.Equal(t, uint64(1), result.FindTable("t_checksum").Options.MySQL.Checksum)
			},
		},
		{
			name:     "Generated and enum",
			database: "test_generated_enum",
			schema: []string{
				`CREATE TABLE orders (
					id INT PRIMARY KEY,
					quantity INT NOT NULL,
					unit_price DECIMAL(10,2) NOT NULL,
					total_price DECIMAL(10,2) AS (quantity * unit_price) STORED,
					total_price_virtual DECIMAL(10,2) AS (quantity * unit_price) VIRTUAL
				) ENGINE=InnoDB`,
				`CREATE TABLE subscriptions (
					id INT PRIMARY KEY,
					status ENUM('active', 'inactive', 'pending', 'canceled') NOT NULL,
					tier SET('free', 'basic', 'premium', 'enterprise') DEFAULT 'free'
				) ENGINE=InnoDB`,
			},
			verify: func(t *testing.T, result *core.Database) {
				t.Helper()
				ordersTable := result.FindTable("orders")
				require.NotNil(t, ordersTable)
				totalPriceCol := ordersTable.FindColumn("total_price")
				require.NotNil(t, totalPriceCol)
				require.True(t, totalPriceCol.IsGenerated)
				require.Equal(t, core.GenerationStored, totalPriceCol.GenerationStorage)
				totalPriceVirtualCol := ordersTable.FindColumn("total_price_virtual")
				require.NotNil(t, totalPriceVirtualCol)
				require.Equal(t, core.GenerationVirtual, totalPriceVirtualCol.GenerationStorage)
				subsTable := result.FindTable("subscriptions")
				require.NotNil(t, subsTable)
				statusCol := subsTable.FindColumn("status")
				require.NotNil(t, statusCol)
				require.Equal(t, core.DataTypeEnum, statusCol.Type)
				require.Equal(t, []string{"active", "inactive", "pending", "canceled"}, statusCol.EnumValues)
				tierCol := subsTable.FindColumn("tier")
				require.NotNil(t, tierCol)
				require.Contains(t, tierCol.EnumValues, "enterprise")
			},
		},
	}

	for _, sc := range scenarios {
		sc := sc
		t.Run(sc.name, func(t *testing.T) {
			_, err = db.Exec("DROP DATABASE IF EXISTS " + sc.database)
			require.NoError(t, err)
			_, err = db.Exec("CREATE DATABASE " + sc.database)
			require.NoError(t, err)
			_, err = db.Exec("USE " + sc.database)
			require.NoError(t, err)
			for _, stmt := range sc.schema {
				_, err = db.Exec(stmt)
				require.NoError(t, err)
			}
			result, err := intr.Introspect(ctx, db)
			require.NoError(t, err)
			require.NotNil(t, result)
			sc.verify(t, result)
		})
	}
}

func TestMariaDBVersionDetection(t *testing.T) {
	ctx := context.Background()

	testVersions := []string{"11.0.3", "10.11"}

	for _, version := range testVersions {
		t.Run("MariaDB_"+version, func(t *testing.T) {
			mariaDBContainer, err := mariadbcontainer.Run(ctx, "mariadb:"+version)
			require.NoError(t, err)
			defer func() {
				require.NoError(t, mariaDBContainer.Terminate(ctx))
			}()

			connStr, err := mariaDBContainer.ConnectionString(ctx)
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

			intr, err := introspect.NewIntrospecter(core.DialectMariaDB)
			require.NoError(t, err)

			_, err = db.Exec("USE test_version")
			require.NoError(t, err)

			result, err := intr.Introspect(ctx, db)
			require.NoError(t, err)
			require.NotNil(t, result)
			require.Equal(t, core.DialectMariaDB, result.Dialect)
		})
	}
}

func TestMariaDBInvisibleMetadata(t *testing.T) {
	ctx := context.Background()

	mariaDBContainer, err := mariadbcontainer.Run(ctx, "mariadb:11.0.3")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, mariaDBContainer.Terminate(ctx))
	}()

	connStr, err := mariaDBContainer.ConnectionString(ctx)
	require.NoError(t, err)

	db, err := sql.Open("mysql", connStr)
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE DATABASE test_invisible_meta")
	require.NoError(t, err)

	_, err = db.Exec("USE test_invisible_meta")
	require.NoError(t, err)

	_, err = db.Exec(`
		CREATE TABLE users (
			id INT PRIMARY KEY,
			name VARCHAR(255),
			secret_data VARCHAR(255) INVISIBLE
		) ENGINE=InnoDB
	`)
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

	intr, err := introspect.NewIntrospecter(core.DialectMariaDB)
	require.NoError(t, err)

	_, err = db.Exec("USE test_invisible_meta")
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

	productsTable := result.FindTable("products")
	require.NotNil(t, productsTable)

	visibleIdx := productsTable.FindIndex("idx_sku")
	require.NotNil(t, visibleIdx)
	require.Equal(t, core.IndexVisible, visibleIdx.Visibility)

	invisibleIdx := productsTable.FindIndex("idx_name")
	require.NotNil(t, invisibleIdx)
	require.Equal(t, core.IndexInvisible, invisibleIdx.Visibility)
}

func TestMariaDBSpecificOptions(t *testing.T) {
	ctx := context.Background()

	mariaDBContainer, err := mariadbcontainer.Run(ctx, "mariadb:11.0.3")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, mariaDBContainer.Terminate(ctx))
	}()

	connStr, err := mariaDBContainer.ConnectionString(ctx)
	require.NoError(t, err)

	db, err := sql.Open("mysql", connStr)
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE DATABASE test_mariadb_opts")
	require.NoError(t, err)

	_, err = db.Exec("USE test_mariadb_opts")
	require.NoError(t, err)

	_, err = db.Exec(`
		CREATE TABLE aria_table (
			id INT PRIMARY KEY,
			data VARCHAR(255)
		) ENGINE=Aria
		  PAGE_CHECKSUM=1
		  TRANSACTIONAL=1
		  COMMENT 'Aria table with MariaDB options'
	`)
	require.NoError(t, err)

	_, err = db.Exec(`
		CREATE TABLE versioning_table (
			id INT PRIMARY KEY,
			data VARCHAR(255)
		) ENGINE=InnoDB
		  WITH SYSTEM VERSIONING
	`)
	require.NoError(t, err)

	_, err = db.Exec(`
		CREATE TABLE sequence_table (
			id INT PRIMARY KEY
		) ENGINE=InnoDB
	`)
	require.NoError(t, err)

	_, err = db.Exec(`
		CREATE TABLE encrypted_table (
			id INT PRIMARY KEY,
			data VARCHAR(255)
		) ENGINE=InnoDB
		  ENCRYPTION_KEY_ID=1
	`)
	require.NoError(t, err)

	intr, err := introspect.NewIntrospecter(core.DialectMariaDB)
	require.NoError(t, err)

	_, err = db.Exec("USE test_mariadb_opts")
	require.NoError(t, err)

	result, err := intr.Introspect(ctx, db)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Len(t, result.Tables, 4)

	ariaTable := result.FindTable("aria_table")
	require.NotNil(t, ariaTable)
	require.NotNil(t, ariaTable.Options.MySQL)
	require.Equal(t, "Aria", ariaTable.Options.MySQL.Engine)

	versioningTable := result.FindTable("versioning_table")
	require.NotNil(t, versioningTable)
	require.NotNil(t, versioningTable.Options.MariaDB)

	sequenceTable := result.FindTable("sequence_table")
	require.NotNil(t, sequenceTable)

	encryptedTable := result.FindTable("encrypted_table")
	require.NotNil(t, encryptedTable)
}

func TestMariaDBAriaStorageEngine(t *testing.T) {
	ctx := context.Background()

	mariaDBContainer, err := mariadbcontainer.Run(ctx, "mariadb:11.0.3")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, mariaDBContainer.Terminate(ctx))
	}()

	connStr, err := mariaDBContainer.ConnectionString(ctx)
	require.NoError(t, err)

	db, err := sql.Open("mysql", connStr)
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE DATABASE test_aria")
	require.NoError(t, err)

	_, err = db.Exec("USE test_aria")
	require.NoError(t, err)

	_, err = db.Exec(`
		CREATE TABLE aria_logs (
			id INT AUTO_INCREMENT PRIMARY KEY,
			message TEXT,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		) ENGINE=Aria
		  PAGE_CHECKSUM=1
		  TRANSACTIONAL=1
		  ROW_FORMAT=DYNAMIC
		  DELAY_KEY_WRITE=1
	`)
	require.NoError(t, err)

	intr, err := introspect.NewIntrospecter(core.DialectMariaDB)
	require.NoError(t, err)

	_, err = db.Exec("USE test_aria")
	require.NoError(t, err)

	result, err := intr.Introspect(ctx, db)
	require.NoError(t, err)
	require.NotNil(t, result)

	ariaLogsTable := result.FindTable("aria_logs")
	require.NotNil(t, ariaLogsTable)
	require.NotNil(t, ariaLogsTable.Options.MySQL)
	require.Equal(t, "Aria", ariaLogsTable.Options.MySQL.Engine)
	require.Equal(t, "DYNAMIC", ariaLogsTable.Options.MySQL.RowFormat)
	require.Equal(t, uint64(1), ariaLogsTable.Options.MySQL.DelayKeyWrite)
}

func TestMariaDBMySQLOptions(t *testing.T) {
	ctx := context.Background()

	mariaDBContainer, err := mariadbcontainer.Run(ctx, "mariadb:11.0.3")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, mariaDBContainer.Terminate(ctx))
	}()

	connStr, err := mariaDBContainer.ConnectionString(ctx)
	require.NoError(t, err)

	db, err := sql.Open("mysql", connStr)
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE DATABASE test_mysql_opts")
	require.NoError(t, err)

	_, err = db.Exec("USE test_mysql_opts")
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
			name: "Aria with page checksum and transactional",
			schema: `
				CREATE TABLE t_aria (
					id INT PRIMARY KEY,
					data VARCHAR(255)
				) ENGINE=Aria
				  PAGE_CHECKSUM=1
				  TRANSACTIONAL=1
			`,
			verify: func(t *testing.T, db *core.Database) {
				t.Helper()
				tbl := db.FindTable("t_aria")
				require.NotNil(t, tbl)
				opts := tbl.Options.MySQL
				require.Equal(t, "Aria", opts.Engine)
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
			name: "Table with auto increment",
			schema: `
				CREATE TABLE t_auto_inc (
					id BIGINT PRIMARY KEY,
					data VARCHAR(255)
				) ENGINE=InnoDB
				  AUTO_INCREMENT=100000
			`,
			verify: func(t *testing.T, db *core.Database) {
				t.Helper()
				tbl := db.FindTable("t_auto_inc")
				require.NotNil(t, tbl)
				opts := tbl.Options.MySQL
				require.Equal(t, uint64(100000), opts.AutoIncrement)
			},
		},
		{
			name: "Table with row format",
			schema: `
				CREATE TABLE t_rowfmt (
					id INT PRIMARY KEY,
					data VARCHAR(255)
				) ENGINE=InnoDB
				  ROW_FORMAT=Compact
			`,
			verify: func(t *testing.T, db *core.Database) {
				t.Helper()
				tbl := db.FindTable("t_rowfmt")
				require.NotNil(t, tbl)
				opts := tbl.Options.MySQL
				require.Equal(t, "Compact", opts.RowFormat)
			},
		},
		{
			name: "Table with avg row length hints",
			schema: `
				CREATE TABLE t_avg (
					id INT PRIMARY KEY,
					data VARCHAR(255)
				) ENGINE=InnoDB
				  AVG_ROW_LENGTH=128
				  MAX_ROWS=500000
				  MIN_ROWS=10
			`,
			verify: func(t *testing.T, db *core.Database) {
				t.Helper()
				tbl := db.FindTable("t_avg")
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
		{
			name: "MyISAM with pack keys",
			schema: `
				CREATE TABLE t_myisam_pk (
					id INT PRIMARY KEY,
					data VARCHAR(255)
				) ENGINE=MyISAM
				  PACK_KEYS=1
			`,
			verify: func(t *testing.T, db *core.Database) {
				t.Helper()
				tbl := db.FindTable("t_myisam_pk")
				require.NotNil(t, tbl)
				opts := tbl.Options.MySQL
				require.Equal(t, "MyISAM", opts.Engine)
				require.Equal(t, "1", opts.PackKeys)
			},
		},
		{
			name: "Table with data directory",
			schema: `
				CREATE TABLE t_datadir (
					id INT PRIMARY KEY,
					data VARCHAR(255)
				) ENGINE=MyISAM
				  DATA DIRECTORY='/var/lib/mysql-data'
			`,
			verify: func(t *testing.T, db *core.Database) {
				t.Helper()
				tbl := db.FindTable("t_datadir")
				require.NotNil(t, tbl)
				opts := tbl.Options.MySQL
				require.Equal(t, "/var/lib/mysql-data", opts.DataDirectory)
			},
		},
		{
			name: "Table with index directory",
			schema: `
				CREATE TABLE t_idxdir (
					id INT PRIMARY KEY,
					data VARCHAR(255)
				) ENGINE=MyISAM
				  INDEX DIRECTORY='/var/lib/mysql-index'
			`,
			verify: func(t *testing.T, db *core.Database) {
				t.Helper()
				tbl := db.FindTable("t_idxdir")
				require.NotNil(t, tbl)
				opts := tbl.Options.MySQL
				require.Equal(t, "/var/lib/mysql-index", opts.IndexDirectory)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := db.Exec(tc.schema)
			require.NoError(t, err)
		})
	}

	intr, err := introspect.NewIntrospecter(core.DialectMariaDB)
	require.NoError(t, err)

	_, err = db.Exec("USE test_mysql_opts")
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

func TestMariaDBFederatedOptions(t *testing.T) {
	ctx := context.Background()

	mariaDBContainer, err := mariadbcontainer.Run(ctx, "mariadb:11.0.3")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, mariaDBContainer.Terminate(ctx))
	}()

	connStr, err := mariaDBContainer.ConnectionString(ctx)
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
		  COMMENT 'Federated table'
	`)
	require.NoError(t, err)

	intr, err := introspect.NewIntrospecter(core.DialectMariaDB)
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
	require.Equal(t, "Federated table", federatedTable.Comment)
}

func TestMariaDBColumnOptions(t *testing.T) {
	ctx := context.Background()

	mariaDBContainer, err := mariadbcontainer.Run(ctx, "mariadb:11.0.3")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, mariaDBContainer.Terminate(ctx))
	}()

	connStr, err := mariaDBContainer.ConnectionString(ctx)
	require.NoError(t, err)

	db, err := sql.Open("mysql", connStr)
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE DATABASE test_col_opts")
	require.NoError(t, err)

	_, err = db.Exec("USE test_col_opts")
	require.NoError(t, err)

	_, err = db.Exec(`
		CREATE TABLE test_columns (
			id INT PRIMARY KEY,
			fixed_col VARCHAR(100) COLUMN_FORMAT FIXED,
			dynamic_col VARCHAR(100) COLUMN_FORMAT DYNAMIC,
			default_col VARCHAR(100) COLUMN_FORMAT DEFAULT,
			disk_col INT STORAGE DISK,
			memory_col INT STORAGE MEMORY,
			regular_col VARCHAR(255)
		) ENGINE=InnoDB
	`)
	require.NoError(t, err)

	intr, err := introspect.NewIntrospecter(core.DialectMariaDB)
	require.NoError(t, err)

	_, err = db.Exec("USE test_col_opts")
	require.NoError(t, err)

	result, err := intr.Introspect(ctx, db)
	require.NoError(t, err)
	require.NotNil(t, result)

	testTable := result.FindTable("test_columns")
	require.NotNil(t, testTable)

	fixedCol := testTable.FindColumn("fixed_col")
	require.NotNil(t, fixedCol)
	require.NotNil(t, fixedCol.MySQL)
	require.Equal(t, "FIXED", fixedCol.MySQL.ColumnFormat)

	dynamicCol := testTable.FindColumn("dynamic_col")
	require.NotNil(t, dynamicCol)
	require.NotNil(t, dynamicCol.MySQL)
	require.Equal(t, "DYNAMIC", dynamicCol.MySQL.ColumnFormat)

	defaultCol := testTable.FindColumn("default_col")
	require.NotNil(t, defaultCol)
	require.NotNil(t, defaultCol.MySQL)
	require.Equal(t, "DEFAULT", defaultCol.MySQL.ColumnFormat)

	diskCol := testTable.FindColumn("disk_col")
	require.NotNil(t, diskCol)
	require.NotNil(t, diskCol.MySQL)
	require.Equal(t, "DISK", diskCol.MySQL.Storage)

	memoryCol := testTable.FindColumn("memory_col")
	require.NotNil(t, memoryCol)
	require.NotNil(t, memoryCol.MySQL)
	require.Equal(t, "MEMORY", memoryCol.MySQL.Storage)

	regularCol := testTable.FindColumn("regular_col")
	require.NotNil(t, regularCol)
	require.NotNil(t, regularCol.MySQL)
}

func TestMariaDBIntrospect_Sequence(t *testing.T) {
	ctx := context.Background()

	mariaDBContainer, err := mariadbcontainer.Run(ctx, "mariadb:11.0.3")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, mariaDBContainer.Terminate(ctx))
	}()

	connStr, err := mariaDBContainer.ConnectionString(ctx)
	require.NoError(t, err)

	db, err := sql.Open("mysql", connStr)
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE DATABASE test_sequence")
	require.NoError(t, err)

	_, err = db.Exec("USE test_sequence")
	require.NoError(t, err)

	_, err = db.Exec(`
		CREATE SEQUENCE test_seq AS INT START WITH 1 INCREMENT BY 1
	`)
	require.NoError(t, err)

	_, err = db.Exec(`
		CREATE TABLE regular_table (
			id INT PRIMARY KEY,
			name VARCHAR(100)
		) ENGINE=InnoDB
	`)
	require.NoError(t, err)

	intr, err := introspect.NewIntrospecter(core.DialectMariaDB)
	require.NoError(t, err)

	_, err = db.Exec("USE test_sequence")
	require.NoError(t, err)

	result, err := intr.Introspect(ctx, db)
	require.NoError(t, err)
	require.NotNil(t, result)

	seqTable := result.FindTable("test_seq")
	require.NotNil(t, seqTable)
	require.NotNil(t, seqTable.Options.MariaDB)
	require.True(t, seqTable.Options.MariaDB.Sequence)

	regularTable := result.FindTable("regular_table")
	require.NotNil(t, regularTable)
}

func TestMariaDBIntrospect_AutoextendSize(t *testing.T) {
	ctx := context.Background()

	mariaDBContainer, err := mariadbcontainer.Run(ctx, "mariadb:11.0.3")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, mariaDBContainer.Terminate(ctx))
	}()

	connStr, err := mariaDBContainer.ConnectionString(ctx)
	require.NoError(t, err)

	db, err := sql.Open("mysql", connStr)
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE DATABASE test_autoextend")
	require.NoError(t, err)

	_, err = db.Exec("USE test_autoextend")
	require.NoError(t, err)

	_, err = db.Exec(`
		CREATE TABLE autoextend_table (
			id INT PRIMARY KEY,
			data VARCHAR(255)
		) ENGINE=InnoDB
		  AUTOEXTEND_SIZE=134217728
	`)
	require.NoError(t, err)

	intr, err := introspect.NewIntrospecter(core.DialectMariaDB)
	require.NoError(t, err)

	_, err = db.Exec("USE test_autoextend")
	require.NoError(t, err)

	result, err := intr.Introspect(ctx, db)
	require.NoError(t, err)
	require.NotNil(t, result)

	autoextendTable := result.FindTable("autoextend_table")
	require.NotNil(t, autoextendTable)
	require.NotNil(t, autoextendTable.Options.MySQL)
	require.Equal(t, "134217728", autoextendTable.Options.MySQL.AutoextendSize)
}

func TestMariaDBEncryptionKeyID(t *testing.T) {
	ctx := context.Background()

	mariaDBContainer, err := mariadbcontainer.Run(ctx, "mariadb:11.0.3")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, mariaDBContainer.Terminate(ctx))
	}()

	connStr, err := mariaDBContainer.ConnectionString(ctx)
	require.NoError(t, err)

	db, err := sql.Open("mysql", connStr)
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE DATABASE test_encr_key")
	require.NoError(t, err)

	_, err = db.Exec("USE test_encr_key")
	require.NoError(t, err)

	_, err = db.Exec(`
		CREATE TABLE t1 (
			id INT PRIMARY KEY,
			data VARCHAR(255)
		) ENGINE=InnoDB
		  ENCRYPTION_KEY_ID=1
	`)
	require.NoError(t, err)

	_, err = db.Exec(`
		CREATE TABLE t2 (
			id INT PRIMARY KEY,
			data VARCHAR(255)
		) ENGINE=InnoDB
		  ENCRYPTION_KEY_ID=5
	`)
	require.NoError(t, err)

	intr, err := introspect.NewIntrospecter(core.DialectMariaDB)
	require.NoError(t, err)

	_, err = db.Exec("USE test_encr_key")
	require.NoError(t, err)

	result, err := intr.Introspect(ctx, db)
	require.NoError(t, err)
	require.NotNil(t, result)

	tbl1 := result.FindTable("t1")
	require.NotNil(t, tbl1)
	require.NotNil(t, tbl1.Options.MariaDB)
	require.NotNil(t, tbl1.Options.MariaDB.EncryptionKeyID)
	require.Equal(t, 1, *tbl1.Options.MariaDB.EncryptionKeyID)

	tbl2 := result.FindTable("t2")
	require.NotNil(t, tbl2)
	require.NotNil(t, tbl2.Options.MariaDB)
	require.NotNil(t, tbl2.Options.MariaDB.EncryptionKeyID)
	require.Equal(t, 5, *tbl2.Options.MariaDB.EncryptionKeyID)
}

func TestMariaDBStorageFlags(t *testing.T) {
	ctx := context.Background()

	mariaDBContainer, err := mariadbcontainer.Run(ctx, "mariadb:11.0.3")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, mariaDBContainer.Terminate(ctx))
	}()

	connStr, err := mariaDBContainer.ConnectionString(ctx)
	require.NoError(t, err)

	db, err := sql.Open("mysql", connStr)
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE DATABASE test_storage_flags")
	require.NoError(t, err)

	_, err = db.Exec("USE test_storage_flags")
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

	_, err = db.Exec(`
		CREATE TABLE csv_enabled (
			id INT PRIMARY KEY,
			data VARCHAR(255)
		) ENGINE=CSV
		  IETF_QUOTES=1
	`)
	require.NoError(t, err)

	_, err = db.Exec(`
		CREATE TABLE csv_disabled (
			id INT PRIMARY KEY,
			data VARCHAR(255)
		) ENGINE=CSV
		  IETF_QUOTES=0
	`)
	require.NoError(t, err)

	_, err = db.Exec(`
		CREATE TABLE versioned_table (
			id INT PRIMARY KEY,
			data VARCHAR(255)
		) ENGINE=InnoDB
		  WITH SYSTEM VERSIONING
	`)
	require.NoError(t, err)

	_, err = db.Exec(`
		CREATE TABLE plain_table (
			id INT PRIMARY KEY,
			data VARCHAR(255)
		) ENGINE=InnoDB
	`)
	require.NoError(t, err)

	intr, err := introspect.NewIntrospecter(core.DialectMariaDB)
	require.NoError(t, err)

	_, err = db.Exec("USE test_storage_flags")
	require.NoError(t, err)

	result, err := intr.Introspect(ctx, db)
	require.NoError(t, err)
	require.NotNil(t, result)

	t1 := result.FindTable("t1")
	require.NotNil(t, t1)
	require.Equal(t, "NO", t1.Options.MySQL.InsertMethod)
	t2 := result.FindTable("t2")
	require.NotNil(t, t2)
	require.Equal(t, "FIRST", t2.Options.MySQL.InsertMethod)
	t3 := result.FindTable("t3")
	require.NotNil(t, t3)
	require.Equal(t, "LAST", t3.Options.MySQL.InsertMethod)
	csvEnabled := result.FindTable("csv_enabled")
	require.NotNil(t, csvEnabled)
	require.True(t, csvEnabled.Options.MySQL.IETFQuotes)
	csvDisabled := result.FindTable("csv_disabled")
	require.NotNil(t, csvDisabled)
	require.False(t, csvDisabled.Options.MySQL.IETFQuotes)
	versionedTable := result.FindTable("versioned_table")
	require.NotNil(t, versionedTable)
	require.True(t, versionedTable.Options.MariaDB.WithSystemVersioning)
	plainTable := result.FindTable("plain_table")
	require.NotNil(t, plainTable)
	require.False(t, plainTable.Options.MariaDB.WithSystemVersioning)
}

func TestMariaDBPassword(t *testing.T) {
	ctx := context.Background()

	mariaDBContainer, err := mariadbcontainer.Run(ctx, "mariadb:11.0.3")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, mariaDBContainer.Terminate(ctx))
	}()

	connStr, err := mariaDBContainer.ConnectionString(ctx)
	require.NoError(t, err)

	db, err := sql.Open("mysql", connStr)
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE DATABASE test_password")
	require.NoError(t, err)

	_, err = db.Exec("USE test_password")
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
		  CONNECTION='mysql://user@remote_host:3306/testdb/remote_users'
		  PASSWORD='secretpassword'
		  COMMENT 'Federated table'
	`)
	require.NoError(t, err)

	intr, err := introspect.NewIntrospecter(core.DialectMariaDB)
	require.NoError(t, err)

	_, err = db.Exec("USE test_password")
	require.NoError(t, err)

	result, err := intr.Introspect(ctx, db)
	require.NoError(t, err)
	require.NotNil(t, result)

	federatedTable := result.FindTable("federated_table")
	require.NotNil(t, federatedTable)
	require.NotNil(t, federatedTable.Options.MySQL)
	require.Equal(t, "FEDERATED", federatedTable.Options.MySQL.Engine)
	require.Equal(t, "mysql://user@remote_host:3306/testdb/remote_users", federatedTable.Options.MySQL.Connection)
	require.Equal(t, "secretpassword", federatedTable.Options.MySQL.Password)
}

func findConstraintByType(t *core.Table, ct core.ConstraintType) *core.Constraint {
	for _, c := range t.Constraints {
		if c.Type == ct {
			return c
		}
	}
	return nil
}
