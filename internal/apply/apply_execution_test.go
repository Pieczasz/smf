package apply

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TODO: This file starts a fresh MySQL container via setupMySQL(t) in many separate top-level tests.
// Spinning up containers repeatedly can make the test suite very slow/flaky.
// Consider reusing a single container per package (e.g., via TestMain, sync.Once, or a shared helper) and cleaning schemas between subtests.
func TestApplierApplyDryRunIntegration(t *testing.T) {
	if testing.Short() {

		t.Skip("skipping integration test in short mode")
	}

	tc := setupMySQL(t)
	ctx := context.Background()

	t.Run("dry run prints statements and does not execute", func(t *testing.T) {
		var buf bytes.Buffer
		applier := NewApplier(Options{
			DSN:    tc.dsn,
			DryRun: true,
			Out:    &buf,
		})
		require.NoError(t, applier.Connect(ctx))
		defer applier.Close()

		stmts := []string{"CREATE TABLE dry_run_test (id INT PRIMARY KEY)"}
		preflight := applier.PreflightChecks(stmts, false)
		err := applier.Apply(ctx, stmts, preflight)
		assert.NoError(t, err)

		output := buf.String()
		assert.Contains(t, output, "DRY RUN MODE")
		assert.Contains(t, output, "CREATE TABLE")

		var count int
		err = tc.db.QueryRow("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='testdb' AND table_name='dry_run_test'").Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 0, count)
	})

	t.Run("dry run with destructive ops fails without --unsafe", func(t *testing.T) {
		var buf bytes.Buffer
		applier := NewApplier(Options{
			DSN:    tc.dsn,
			DryRun: true,
			Unsafe: false,
			Out:    &buf,
		})
		require.NoError(t, applier.Connect(ctx))
		defer applier.Close()

		stmts := []string{"DROP TABLE IF EXISTS nonexistent_table"}
		preflight := applier.PreflightChecks(stmts, false)
		err := applier.Apply(ctx, stmts, preflight)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "destructive")
	})

	t.Run("dry run with destructive ops succeeds with --unsafe", func(t *testing.T) {
		var buf bytes.Buffer
		applier := NewApplier(Options{
			DSN:    tc.dsn,
			DryRun: true,
			Unsafe: true,
			Out:    &buf,
		})
		require.NoError(t, applier.Connect(ctx))
		defer applier.Close()

		stmts := []string{"DROP TABLE IF EXISTS nonexistent_table"}
		preflight := applier.PreflightChecks(stmts, true)
		err := applier.Apply(ctx, stmts, preflight)
		assert.NoError(t, err)
	})
}

func TestApplierApplyTransactionIntegrationCreateTable(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	tc := setupMySQL(t)
	ctx := context.Background()

	var buf bytes.Buffer
	applier := NewApplier(Options{
		DSN:                   tc.dsn,
		Transaction:           true,
		AllowNonTransactional: true,
		SkipConfirmation:      true,
		Out:                   &buf,
	})
	require.NoError(t, applier.Connect(ctx))
	defer applier.Close()

	stmts := []string{"CREATE TABLE tx_test1 (id INT PRIMARY KEY, name VARCHAR(100))"}
	preflight := applier.PreflightChecks(stmts, false)
	err := applier.Apply(ctx, stmts, preflight)
	require.NoError(t, err)

	output := buf.String()
	assert.Contains(t, output, "Migration complete!")

	var count int
	err = tc.db.QueryRow("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='testdb' AND table_name='tx_test1'").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestApplierApplyTransactionIntegrationDML(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	tc := setupMySQL(t)
	ctx := context.Background()

	_, err := tc.db.Exec("CREATE TABLE IF NOT EXISTS tx_insert_test (id INT PRIMARY KEY, val VARCHAR(100))")
	require.NoError(t, err)

	var buf bytes.Buffer
	applier := NewApplier(Options{
		DSN:              tc.dsn,
		Transaction:      true,
		SkipConfirmation: true,
		Out:              &buf,
	})
	require.NoError(t, applier.Connect(ctx))
	defer applier.Close()

	stmts := []string{
		"INSERT INTO tx_insert_test (id, val) VALUES (1, 'hello')",
		"INSERT INTO tx_insert_test (id, val) VALUES (2, 'world')",
	}
	preflight := applier.PreflightChecks(stmts, false)
	err = applier.Apply(ctx, stmts, preflight)
	require.NoError(t, err)

	var count int
	err = tc.db.QueryRow("SELECT COUNT(*) FROM tx_insert_test").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 2, count)
}

func TestApplierApplyTransactionIntegrationRollback(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	tc := setupMySQL(t)
	ctx := context.Background()

	_, err := tc.db.Exec("CREATE TABLE IF NOT EXISTS tx_rollback_test (id INT PRIMARY KEY, val VARCHAR(100))")
	require.NoError(t, err)
	_, err = tc.db.Exec("DELETE FROM tx_rollback_test")
	require.NoError(t, err)

	var buf bytes.Buffer
	applier := NewApplier(Options{
		DSN:              tc.dsn,
		Transaction:      true,
		SkipConfirmation: true,
		Out:              &buf,
	})
	require.NoError(t, applier.Connect(ctx))
	defer applier.Close()

	stmts := []string{
		"INSERT INTO tx_rollback_test (id, val) VALUES (1, 'committed?')",
		"INSERT INTO tx_rollback_test (id, val) VALUES (1, 'duplicate!')",
	}
	preflight := applier.PreflightChecks(stmts, false)
	err = applier.Apply(ctx, stmts, preflight)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "rolled back")

	var count int
	err = tc.db.QueryRow("SELECT COUNT(*) FROM tx_rollback_test").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestApplierApplyTransactionIntegrationNonTXRejected(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	tc := setupMySQL(t)
	ctx := context.Background()

	var buf bytes.Buffer
	applier := NewApplier(Options{
		DSN:                   tc.dsn,
		Transaction:           true,
		AllowNonTransactional: false,
		SkipConfirmation:      true,
		Out:                   &buf,
	})
	require.NoError(t, applier.Connect(ctx))
	defer applier.Close()

	stmts := []string{"CREATE TABLE should_not_exist_table (id INT PRIMARY KEY)"}
	preflight := applier.PreflightChecks(stmts, false)
	err := applier.Apply(ctx, stmts, preflight)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "non-transactional")
}

func TestApplierApplyNonTransactionIntegrationDDL(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	tc := setupMySQL(t)
	ctx := context.Background()

	var buf bytes.Buffer
	applier := NewApplier(Options{
		DSN:              tc.dsn,
		Transaction:      false,
		SkipConfirmation: true,
		Out:              &buf,
	})
	require.NoError(t, applier.Connect(ctx))
	defer applier.Close()

	stmts := []string{
		"CREATE TABLE notx_test1 (id INT PRIMARY KEY, name VARCHAR(255))",
		"ALTER TABLE notx_test1 ADD COLUMN email VARCHAR(255)",
	}
	preflight := applier.PreflightChecks(stmts, false)
	err := applier.Apply(ctx, stmts, preflight)
	require.NoError(t, err)

	output := buf.String()
	assert.Contains(t, output, "Migration complete!")
	assert.Contains(t, output, "[1/2] OK")
	assert.Contains(t, output, "[2/2] OK")

	var columnCount int
	err = tc.db.QueryRow(`
		SELECT COUNT(*) FROM information_schema.columns
		WHERE table_schema='testdb' AND table_name='notx_test1'
	`).Scan(&columnCount)
	require.NoError(t, err)
	assert.Equal(t, 3, columnCount)
}

func TestApplierApplyNonTransactionIntegrationPartialFailure(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	tc := setupMySQL(t)
	ctx := context.Background()

	_, err := tc.db.Exec("CREATE TABLE IF NOT EXISTS notx_partial (id INT PRIMARY KEY)")
	require.NoError(t, err)

	var buf bytes.Buffer
	applier := NewApplier(Options{
		DSN:              tc.dsn,
		Transaction:      false,
		SkipConfirmation: true,
		Out:              &buf,
	})
	require.NoError(t, applier.Connect(ctx))
	defer applier.Close()

	stmts := []string{
		"INSERT INTO notx_partial (id) VALUES (100)",
		"INSERT INTO notx_partial (id) VALUES (100)",
		"INSERT INTO notx_partial (id) VALUES (200)",
	}
	preflight := applier.PreflightChecks(stmts, false)
	err = applier.Apply(ctx, stmts, preflight)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "statement 2 failed")
	assert.Contains(t, err.Error(), "1 statements were already applied")

	var count int
	err = tc.db.QueryRow("SELECT COUNT(*) FROM notx_partial WHERE id = 100").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	err = tc.db.QueryRow("SELECT COUNT(*) FROM notx_partial WHERE id = 200").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestApplierApplyNonTransactionIntegrationMultipleDDL(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	tc := setupMySQL(t)
	ctx := context.Background()

	var buf bytes.Buffer
	applier := NewApplier(Options{
		DSN:              tc.dsn,
		Transaction:      false,
		Unsafe:           true,
		SkipConfirmation: true,
		Out:              &buf,
	})
	require.NoError(t, applier.Connect(ctx))
	defer applier.Close()

	stmts := []string{
		"CREATE TABLE notx_multi1 (id INT PRIMARY KEY)",
		"CREATE TABLE notx_multi2 (id INT PRIMARY KEY, ref_id INT)",
		"DROP TABLE notx_multi1",
	}
	preflight := applier.PreflightChecks(stmts, true)
	err := applier.Apply(ctx, stmts, preflight)
	require.NoError(t, err)

	var count int
	err = tc.db.QueryRow("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='testdb' AND table_name='notx_multi1'").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	err = tc.db.QueryRow("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='testdb' AND table_name='notx_multi2'").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

func confirmationTestHelper(ctx context.Context, t *testing.T, tc *testMySQLContainer,
	input string, tableName string, insertVal int, expectedMsg string) {
	t.Helper()
	var buf bytes.Buffer
	applier := NewApplier(Options{
		DSN:         tc.dsn,
		Transaction: false,
		Out:         &buf,
		In:          strings.NewReader(input),
	})
	require.NoError(t, applier.Connect(ctx))
	defer applier.Close()

	createTbl := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id INT PRIMARY KEY)", tableName)
	_, err := tc.db.Exec(createTbl)
	require.NoError(t, err)

	stmts := []string{fmt.Sprintf("INSERT INTO %s (id) VALUES (%d)", tableName, insertVal)}
	preflight := applier.PreflightChecks(stmts, false)
	err = applier.Apply(ctx, stmts, preflight)
	assert.NoError(t, err)
	assert.Contains(t, buf.String(), expectedMsg)
}

func TestApplierApplyConfirmationIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	tc := setupMySQL(t)
	ctx := context.Background()

	t.Run("user confirms with y", func(t *testing.T) {
		confirmationTestHelper(ctx, t, tc, "y\n", "confirm_test_y", 1, "Migration complete!")
	})

	t.Run("user confirms with yes", func(t *testing.T) {
		confirmationTestHelper(ctx, t, tc, "yes\n", "confirm_test_yes", 1, "Migration complete!")
	})

	t.Run("user denies with n", func(t *testing.T) {
		var buf bytes.Buffer
		applier := NewApplier(Options{
			DSN:         tc.dsn,
			Transaction: false,
			Out:         &buf,
			In:          strings.NewReader("n\n"),
		})
		require.NoError(t, applier.Connect(ctx))
		defer applier.Close()

		_, err := tc.db.Exec("CREATE TABLE IF NOT EXISTS confirm_test_n (id INT PRIMARY KEY)")
		require.NoError(t, err)

		stmts := []string{"INSERT INTO confirm_test_n (id) VALUES (999)"}
		preflight := applier.PreflightChecks(stmts, false)
		err = applier.Apply(ctx, stmts, preflight)
		assert.NoError(t, err)
		assert.Contains(t, buf.String(), "Migration canceled.")

		var count int
		err = tc.db.QueryRow("SELECT COUNT(*) FROM confirm_test_n WHERE id = 999").Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 0, count)
	})

	t.Run("empty input denies", func(t *testing.T) {
		confirmationTestHelper(ctx, t, tc, "\n", "confirm_test_empty", 888, "Migration canceled.")
	})

	t.Run("EOF input denies", func(t *testing.T) {
		confirmationTestHelper(ctx, t, tc, "", "confirm_test_eof", 777, "Migration canceled.")
	})
}

func TestApplierFullLifecycleIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	tc := setupMySQL(t)
	ctx := context.Background()

	t.Run("create-insert-alter-query lifecycle", func(t *testing.T) {
		var buf bytes.Buffer
		applier := NewApplier(Options{
			DSN:              tc.dsn,
			Transaction:      false,
			SkipConfirmation: true,
			Out:              &buf,
		})
		require.NoError(t, applier.Connect(ctx))
		defer applier.Close()

		createStmts := applier.ParseStatements("CREATE TABLE lifecycle_test (id INT PRIMARY KEY, name VARCHAR(100));")
		preflight := applier.PreflightChecks(createStmts, false)
		require.NoError(t, applier.Apply(ctx, createStmts, preflight))

		insertStmts := applier.ParseStatements(`
			INSERT INTO lifecycle_test (id, name) VALUES (1, 'Alice');
			INSERT INTO lifecycle_test (id, name) VALUES (2, 'Bob');
		`)
		preflight = applier.PreflightChecks(insertStmts, false)
		require.NoError(t, applier.Apply(ctx, insertStmts, preflight))

		alterStmts := applier.ParseStatements("ALTER TABLE lifecycle_test ADD COLUMN active BOOLEAN DEFAULT TRUE;")
		preflight = applier.PreflightChecks(alterStmts, false)
		require.NoError(t, applier.Apply(ctx, alterStmts, preflight))

		var count int
		err := tc.db.QueryRow("SELECT COUNT(*) FROM lifecycle_test").Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 2, count)

		var colCount int
		err = tc.db.QueryRow(`
			SELECT COUNT(*) FROM information_schema.columns
			WHERE table_schema='testdb' AND table_name='lifecycle_test'
		`).Scan(&colCount)
		require.NoError(t, err)
		assert.Equal(t, 3, colCount)
	})

	t.Run("JSON migration format end to end", func(t *testing.T) {
		migration := jsonMigration{
			Format: "json",
			SQL: []string{
				"CREATE TABLE json_e2e (id INT PRIMARY KEY, description TEXT)",
				"INSERT INTO json_e2e (id, description) VALUES (1, 'test entry')",
			},
		}
		migration.Summary.SQLStatements = 2
		raw, err := json.Marshal(migration)
		require.NoError(t, err)

		var buf bytes.Buffer
		applier := NewApplier(Options{
			DSN:              tc.dsn,
			Transaction:      false,
			SkipConfirmation: true,
			Out:              &buf,
		})
		require.NoError(t, applier.Connect(ctx))
		defer applier.Close()

		stmts := applier.ParseStatements(string(raw))
		preflight := applier.PreflightChecks(stmts, false)
		require.NoError(t, applier.Apply(ctx, stmts, preflight))

		var desc string
		err = tc.db.QueryRow("SELECT description FROM json_e2e WHERE id = 1").Scan(&desc)
		require.NoError(t, err)
		assert.Equal(t, "test entry", desc)
	})
}

func TestApplierEdgeCasesIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	tc := setupMySQL(t)
	ctx := context.Background()

	t.Run("invalid SQL statement fails gracefully", func(t *testing.T) {
		applier, _ := edgeCasesTestHelper(ctx, t, tc, false, false)
		defer applier.Close()

		stmts := []string{"THIS IS NOT VALID SQL AT ALL"}
		preflight := applier.PreflightChecks(stmts, false)
		err := applier.Apply(ctx, stmts, preflight)
		assert.Error(t, err)
	})

	t.Run("empty statements list succeeds", func(t *testing.T) {
		applier, buf := edgeCasesTestHelper(ctx, t, tc, false, false)
		defer applier.Close()

		stmts := []string{}
		preflight := applier.PreflightChecks(stmts, false)
		err := applier.Apply(ctx, stmts, preflight)
		assert.NoError(t, err)
		assert.Contains(t, buf.String(), "Migration complete!")
	})

	t.Run("empty statements with transaction succeeds", func(t *testing.T) {
		applier, _ := edgeCasesTestHelper(ctx, t, tc, false, true)
		defer applier.Close()

		stmts := []string{}
		preflight := applier.PreflightChecks(stmts, false)
		err := applier.Apply(ctx, stmts, preflight)
		assert.NoError(t, err)
	})

	t.Run("context cancellation stops execution", func(t *testing.T) {
		applier, _ := edgeCasesTestHelper(ctx, t, tc, false, false)
		defer applier.Close()

		cancelCtx, cancel := context.WithCancel(ctx)
		cancel()

		stmts := []string{"SELECT SLEEP(10)"}
		preflight := applier.PreflightChecks(stmts, false)
		err := applier.Apply(cancelCtx, stmts, preflight)
		assert.Error(t, err)
	})

	t.Run("context cancellation stops transactional execution", func(t *testing.T) {
		_, err := tc.db.Exec("CREATE TABLE IF NOT EXISTS cancel_tx_test (id INT PRIMARY KEY)")
		require.NoError(t, err)

		applier, _ := edgeCasesTestHelper(ctx, t, tc, false, true)
		defer applier.Close()

		cancelCtx, cancel := context.WithCancel(ctx)
		cancel()

		stmts := []string{"INSERT INTO cancel_tx_test VALUES (1)"}
		preflight := applier.PreflightChecks(stmts, false)
		err = applier.Apply(cancelCtx, stmts, preflight)
		assert.Error(t, err)
	})

	t.Run("displayPreflightChecks with errors array", func(t *testing.T) {
		displayPreflightWithErrorsTest(t)
	})

	t.Run("displayPreflightChecks without db shows no accessible message",
		func(t *testing.T) {
			displayPreflightWithoutDBTest(t)
		})

	t.Run("printf and println work correctly", func(t *testing.T) {
		printfAndPrintlnTest(t)
	})
}

func TestApplierTransactionMatrixIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	tc := setupMySQL(t)
	ctx := context.Background()

	t.Run("tx=true + transactional=true -> uses transaction", func(t *testing.T) {
		_, err := tc.db.Exec("CREATE TABLE IF NOT EXISTS tx_matrix1 (id INT PRIMARY KEY)")
		require.NoError(t, err)
		_, err = tc.db.Exec("DELETE FROM tx_matrix1")
		require.NoError(t, err)

		var buf bytes.Buffer
		applier := NewApplier(Options{
			DSN:              tc.dsn,
			Transaction:      true,
			SkipConfirmation: true,
			Out:              &buf,
		})
		require.NoError(t, applier.Connect(ctx))
		defer applier.Close()

		stmts := []string{"INSERT INTO tx_matrix1 (id) VALUES (1)"}
		preflight := &PreflightResult{IsTransactional: true}
		err = applier.Apply(ctx, stmts, preflight)
		require.NoError(t, err)

		var count int
		err = tc.db.QueryRow("SELECT COUNT(*) FROM tx_matrix1").Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 1, count)
	})

	t.Run("tx=true + transactional=false + allowNonTx=true -> uses non-tx", func(t *testing.T) {
		var buf bytes.Buffer
		applier := NewApplier(Options{
			DSN:                   tc.dsn,
			Transaction:           true,
			AllowNonTransactional: true,
			SkipConfirmation:      true,
			Out:                   &buf,
		})
		require.NoError(t, applier.Connect(ctx))
		defer applier.Close()

		stmts := []string{"CREATE TABLE tx_matrix_nontx (id INT PRIMARY KEY)"}
		preflight := &PreflightResult{IsTransactional: false}
		err := applier.Apply(ctx, stmts, preflight)
		require.NoError(t, err)
		assert.Contains(t, buf.String(), "Migration complete!")
	})

	t.Run("tx=false -> always uses non-tx", func(t *testing.T) {
		_, err := tc.db.Exec("CREATE TABLE IF NOT EXISTS tx_matrix_notx (id INT PRIMARY KEY)")
		require.NoError(t, err)

		var buf bytes.Buffer
		applier := NewApplier(Options{
			DSN:              tc.dsn,
			Transaction:      false,
			SkipConfirmation: true,
			Out:              &buf,
		})
		require.NoError(t, applier.Connect(ctx))
		defer applier.Close()

		stmts := []string{"INSERT INTO tx_matrix_notx (id) VALUES (42)"}
		preflight := &PreflightResult{IsTransactional: true}
		err = applier.Apply(ctx, stmts, preflight)
		require.NoError(t, err)

		var count int
		err = tc.db.QueryRow("SELECT COUNT(*) FROM tx_matrix_notx WHERE id = 42").Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 1, count)
	})
}

func TestApplierComplexMigrationsIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	tc := setupMySQL(t)
	ctx := context.Background()

	t.Run("multi-table schema with foreign keys", func(t *testing.T) {
		var buf bytes.Buffer
		applier := NewApplier(Options{
			DSN:              tc.dsn,
			Transaction:      false,
			SkipConfirmation: true,
			Out:              &buf,
		})
		require.NoError(t, applier.Connect(ctx))
		defer applier.Close()

		multiTableForeignKeysTest(ctx, t, applier, tc)
	})

	t.Run("alter table with index operations", func(t *testing.T) {
		alterTableWithIndexTest(ctx, t, tc)
	})

	t.Run("destructive migration with --unsafe", func(t *testing.T) {
		destructiveMigrationTest(ctx, t, tc)
	})
}

func multiTableForeignKeysTest(ctx context.Context, t *testing.T, applier *Applier,
	tc *testMySQLContainer) {
	t.Helper()
	sqlContent := `
CREATE TABLE departments (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL
);

CREATE TABLE employees (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    dept_id INT,
    CONSTRAINT fk_dept FOREIGN KEY (dept_id) REFERENCES departments(id)
);

INSERT INTO departments (name) VALUES ('Engineering');
INSERT INTO employees (name, dept_id) VALUES ('Alice', 1);
`
	stmts := applier.ParseStatements(sqlContent)
	require.Len(t, stmts, 4)

	preflight := applier.PreflightChecks(stmts, false)
	err := applier.Apply(ctx, stmts, preflight)
	require.NoError(t, err)

	var empName string
	err = tc.db.QueryRow("SELECT e.name FROM employees e JOIN departments d ON " +
		"e.dept_id = d.id WHERE d.name = 'Engineering'").Scan(&empName)
	require.NoError(t, err)
	assert.Equal(t, "Alice", empName)
}

func alterTableWithIndexTest(ctx context.Context, t *testing.T, tc *testMySQLContainer) {
	t.Helper()
	_, err := tc.db.Exec(`CREATE TABLE IF NOT EXISTS idx_test (
		id INT AUTO_INCREMENT PRIMARY KEY,
		email VARCHAR(255),
		name VARCHAR(100)
	)`)
	require.NoError(t, err)

	var buf bytes.Buffer
	applier := NewApplier(Options{
		DSN:              tc.dsn,
		Transaction:      false,
		SkipConfirmation: true,
		Out:              &buf,
	})
	require.NoError(t, applier.Connect(ctx))
	defer applier.Close()

	stmts := []string{
		"CREATE INDEX idx_email ON idx_test (email)",
		"CREATE INDEX idx_name ON idx_test (name)",
	}
	preflight := applier.PreflightChecks(stmts, false)
	err = applier.Apply(ctx, stmts, preflight)
	require.NoError(t, err)

	var idxCount int
	err = tc.db.QueryRow(`
		SELECT COUNT(*) FROM information_schema.statistics
		WHERE table_schema = 'testdb' AND table_name = 'idx_test'
		AND index_name IN ('idx_email', 'idx_name')
	`).Scan(&idxCount)
	require.NoError(t, err)
	assert.Equal(t, 2, idxCount)
}

func destructiveMigrationTest(ctx context.Context, t *testing.T, tc *testMySQLContainer) {
	t.Helper()
	_, err := tc.db.Exec("CREATE TABLE IF NOT EXISTS unsafe_drop_test (id INT PRIMARY KEY)")
	require.NoError(t, err)

	var buf bytes.Buffer
	applier := NewApplier(Options{
		DSN:              tc.dsn,
		Transaction:      false,
		Unsafe:           true,
		SkipConfirmation: true,
		Out:              &buf,
	})
	require.NoError(t, applier.Connect(ctx))
	defer applier.Close()

	stmts := []string{"DROP TABLE unsafe_drop_test"}
	preflight := applier.PreflightChecks(stmts, true)
	err = applier.Apply(ctx, stmts, preflight)
	require.NoError(t, err)

	var count int
	err = tc.db.QueryRow("SELECT COUNT(*) FROM information_schema.tables " +
		"WHERE table_schema='testdb' AND table_name='unsafe_drop_test'").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestApplierStressIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	tc := setupMySQL(t)
	ctx := context.Background()

	t.Run("many statements in sequence", func(t *testing.T) {
		_, err := tc.db.Exec("CREATE TABLE IF NOT EXISTS stress_test (id INT PRIMARY KEY, val INT)")
		require.NoError(t, err)

		var buf bytes.Buffer
		applier := NewApplier(Options{
			DSN:              tc.dsn,
			Transaction:      true,
			SkipConfirmation: true,
			Out:              &buf,
		})
		require.NoError(t, applier.Connect(ctx))
		defer applier.Close()

		stmts := make([]string, 50)
		for i := range stmts {
			stmts[i] = fmt.Sprintf("INSERT INTO stress_test (id, val) VALUES (%d, %d)", i+1, i*10)
		}

		preflight := &PreflightResult{IsTransactional: true}
		err = applier.Apply(ctx, stmts, preflight)
		require.NoError(t, err)

		var count int
		err = tc.db.QueryRow("SELECT COUNT(*) FROM stress_test").Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 50, count)
	})
}

func TestApplierApplyEarlyReturn_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	tc := setupMySQL(t)
	ctx := context.Background()

	t.Run("tx mode rejects non-tx DDL before confirmation", func(t *testing.T) {
		var buf bytes.Buffer
		applier := NewApplier(Options{
			DSN:                   tc.dsn,
			Transaction:           true,
			AllowNonTransactional: false,
			SkipConfirmation:      true,
			Out:                   &buf,
		})
		require.NoError(t, applier.Connect(ctx))
		defer applier.Close()

		stmts := []string{"CREATE TABLE early_return_test (id INT PRIMARY KEY)"}
		preflight := applier.PreflightChecks(stmts, false)
		err := applier.Apply(ctx, stmts, preflight)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "non-transactional")
	})

	t.Run("destructive + unsafe=false rejects in Apply via validatePreflight", func(t *testing.T) {
		var buf bytes.Buffer
		applier := NewApplier(Options{
			DSN:              tc.dsn,
			Transaction:      false,
			Unsafe:           false,
			SkipConfirmation: true,
			Out:              &buf,
		})
		require.NoError(t, applier.Connect(ctx))
		defer applier.Close()

		stmts := []string{"DROP TABLE nonexistent"}
		preflight := applier.PreflightChecks(stmts, false)
		err := applier.Apply(ctx, stmts, preflight)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "destructive")
	})
}

func edgeCasesTestHelper(ctx context.Context, t *testing.T, tc *testMySQLContainer,
	dryRun bool, transaction bool) (*Applier, *bytes.Buffer) {
	t.Helper()
	var buf bytes.Buffer
	applier := NewApplier(Options{
		DSN:              tc.dsn,
		DryRun:           dryRun,
		Transaction:      transaction,
		SkipConfirmation: true,
		Out:              &buf,
	})
	require.NoError(t, applier.Connect(ctx))
	return applier, &buf
}

func displayPreflightWithErrorsTest(t *testing.T) {
	t.Helper()
	var buf bytes.Buffer
	applier := NewApplier(Options{Out: &buf})

	preflight := &PreflightResult{
		Errors:          []string{"some error"},
		IsTransactional: true,
	}
	applier.displayPreflightChecks(preflight)
	assert.Contains(t, buf.String(), "ERROR: some error")
}

func displayPreflightWithoutDBTest(t *testing.T) {
	t.Helper()
	var buf bytes.Buffer
	applier := NewApplier(Options{Out: &buf})

	preflight := &PreflightResult{
		IsTransactional: true,
	}
	applier.displayPreflightChecks(preflight)
	assert.NotContains(t, buf.String(), "Database is accessible")
}

func printfAndPrintlnTest(t *testing.T) {
	t.Helper()
	var buf bytes.Buffer
	applier := NewApplier(Options{Out: &buf})

	applier.printf("hello %s", "world")
	applier.println("foo bar")

	output := buf.String()
	assert.Contains(t, output, "hello world")
	assert.Contains(t, output, "foo bar")
}
