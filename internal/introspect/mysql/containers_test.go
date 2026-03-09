package mysql

import (
	"context"
	"database/sql"
	"log"
	"os"
	"testing"

	mariadbcontainer "github.com/testcontainers/testcontainers-go/modules/mariadb"
	mysqlcontainer "github.com/testcontainers/testcontainers-go/modules/mysql"
)

var (
	sharedMySQLContainer   *sql.DB
	sharedMariaDBContainer *sql.DB
)

func TestMain(m *testing.M) {
	os.Exit(runTests(m))
}

func runTests(m *testing.M) int {
	if testing.Short() {
		return m.Run()
	}

	ctx := context.Background()

	mc, err := mysqlcontainer.Run(ctx, "mysql:8.0")
	if err != nil {
		log.Printf("start MySQL container: %v", err)
		return 1
	}
	defer func() {
		if err := mc.Terminate(ctx); err != nil {
			log.Printf("terminate MySQL container: %v", err)
		}
	}()

	mcConnStr, err := mc.ConnectionString(ctx)
	if err != nil {
		log.Printf("MySQL connection string: %v", err)
		return 1
	}

	sharedMySQLContainer, err = sql.Open("mysql", mcConnStr)
	if err != nil {
		log.Printf("open MySQL DB: %v", err)
		return 1
	}
	defer sharedMySQLContainer.Close()

	mdc, err := mariadbcontainer.Run(ctx, "mariadb:11.0.3")
	if err != nil {
		log.Printf("start MariaDB container: %v", err)
		return 1
	}
	defer func() {
		if err := mdc.Terminate(ctx); err != nil {
			log.Printf("terminate MariaDB container: %v", err)
		}
	}()

	mdcConnStr, err := mdc.ConnectionString(ctx)
	if err != nil {
		log.Printf("MariaDB connection string: %v", err)
		return 1
	}

	sharedMariaDBContainer, err = sql.Open("mysql", mdcConnStr)
	if err != nil {
		log.Printf("open MariaDB DB: %v", err)
		return 1
	}
	defer sharedMariaDBContainer.Close()

	return m.Run()
}
