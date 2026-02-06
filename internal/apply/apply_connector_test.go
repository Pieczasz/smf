package apply

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mysql"
)

type testMySQLContainer struct {
	container *mysql.MySQLContainer
	dsn       string
	db        *sql.DB
}

func TestApplierConnectIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	tc := setupMySQL(t)
	ctx := context.Background()

	t.Run("successful connection", func(t *testing.T) {
		applier := NewApplier(Options{DSN: tc.dsn})
		err := applier.Connect(ctx)
		require.NoError(t, err)
		require.NoError(t, applier.Close())
	})

	t.Run("invalid DSN fails", func(t *testing.T) {
		applier := NewApplier(Options{DSN: "invalid:user@tcp(127.0.0.1:1)/nope"})
		err := applier.Connect(ctx)
		assert.Error(t, err)
		assert.NoError(t, applier.Close())
	})

	t.Run("close without connect is safe", func(t *testing.T) {
		applier := NewApplier(Options{DSN: tc.dsn})
		assert.NoError(t, applier.Close())
	})

	t.Run("double close is safe", func(t *testing.T) {
		applier := NewApplier(Options{DSN: tc.dsn})
		require.NoError(t, applier.Connect(ctx))
		require.NoError(t, applier.Close())
		_ = applier.Close()
	})
}

func setupMySQL(t *testing.T) *testMySQLContainer {
	t.Helper()
	ctx := context.Background()

	mysqlContainer, err := mysql.Run(ctx, "mysql:8.0",
		mysql.WithDatabase("testdb"),
		mysql.WithUsername("root"),
		mysql.WithPassword("testpass"),
	)
	require.NoError(t, err, "failed to start MySQL container")

	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(mysqlContainer); err != nil {
			t.Logf("failed to terminate container: %v", err)
		}
	})

	dsn, err := mysqlContainer.ConnectionString(ctx, "parseTime=true")
	require.NoError(t, err, "failed to get connection string")

	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err, "failed to open direct DB connection")
	require.NoError(t, db.PingContext(ctx), "failed to ping database")
	t.Cleanup(func() {
		err := db.Close()
		if err != nil {
			t.Errorf("failed to close DB connection: %v", err)
		}
	})

	return &testMySQLContainer{
		container: mysqlContainer,
		dsn:       dsn,
		db:        db,
	}
}
