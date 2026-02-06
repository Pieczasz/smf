package apply

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewApplierDefaults(t *testing.T) {
	t.Run("default out is discard", func(t *testing.T) {
		a := NewApplier(Options{})
		assert.NotNil(t, a.out)
		assert.NotNil(t, a.in)
		assert.NotNil(t, a.analyzer)
	})

	t.Run("custom out and in are propagated", func(t *testing.T) {
		var buf bytes.Buffer
		in := strings.NewReader("test")
		a := NewApplier(Options{Out: &buf, In: in})
		assert.Equal(t, &buf, a.out)
		assert.Equal(t, in, a.in)
	})
}

func TestApplierTimingOutputIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	tc := setupMySQL(t)
	ctx := context.Background()

	t.Run("execution timing is displayed", func(t *testing.T) {
		_, err := tc.db.Exec("CREATE TABLE IF NOT EXISTS timing_test (id INT PRIMARY KEY)")
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

		stmts := []string{"INSERT INTO timing_test (id) VALUES (1)"}
		preflight := applier.PreflightChecks(stmts, false)
		require.NoError(t, applier.Apply(ctx, stmts, preflight))

		output := buf.String()
		assert.Contains(t, output, "OK")
		assert.Contains(t, output, "s)")
	})

	t.Run("transaction timing is displayed", func(t *testing.T) {
		_, err := tc.db.Exec("CREATE TABLE IF NOT EXISTS timing_tx_test (id INT PRIMARY KEY)")
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

		stmts := []string{"INSERT INTO timing_tx_test (id) VALUES (1)"}
		preflight := &PreflightResult{IsTransactional: true}
		require.NoError(t, applier.Apply(ctx, stmts, preflight))

		output := buf.String()
		assert.Contains(t, output, "OK")
		assert.Contains(t, output, "s)")
	})
}

func TestDisplayPreflightChecksAllPaths(t *testing.T) {
	t.Run("with errors in preflight", func(t *testing.T) {
		var buf bytes.Buffer
		applier := NewApplier(Options{Out: &buf})

		applier.statements = []string{"SELECT 1"}

		preflight := &PreflightResult{
			Errors: []string{"error one", "error two"},
			Warnings: []Warning{
				{Level: WarnCaution, Message: "caution msg"},
				{Level: WarnDanger, Message: "danger msg"},
			},
			IsTransactional: false,
			NonTxReasons:    []string{"reason one"},
		}
		applier.displayPreflightChecks(preflight)

		output := buf.String()
		assert.Contains(t, output, "ERROR: error one")
		assert.Contains(t, output, "ERROR: error two")
		assert.Contains(t, output, "WARNING: caution msg")
		assert.Contains(t, output, "DANGER: danger msg")
		assert.Contains(t, output, "NOT transaction-safe")
		assert.Contains(t, output, "reason one")
	})

	t.Run("transactional does not show non-tx warning", func(t *testing.T) {
		var buf bytes.Buffer
		applier := NewApplier(Options{Out: &buf})
		preflight := &PreflightResult{IsTransactional: true}
		applier.displayPreflightChecks(preflight)
		assert.NotContains(t, buf.String(), "NOT transaction-safe")
	})

	t.Run("no statements and no errors shows valid SQL", func(t *testing.T) {
		var buf bytes.Buffer
		applier := NewApplier(Options{Out: &buf})
		preflight := &PreflightResult{IsTransactional: true}
		applier.displayPreflightChecks(preflight)
		assert.Contains(t, buf.String(), "All migrations are valid SQL")
	})

	t.Run("with statements shows valid SQL", func(t *testing.T) {
		var buf bytes.Buffer
		applier := NewApplier(Options{Out: &buf})
		applier.statements = []string{"SELECT 1"}
		preflight := &PreflightResult{
			IsTransactional: true,
		}
		applier.displayPreflightChecks(preflight)
		assert.Contains(t, buf.String(), "All migrations are valid SQL")
	})
}

func TestDisplayStatements(t *testing.T) {
	var buf bytes.Buffer
	applier := NewApplier(Options{Out: &buf})
	applier.displayStatements([]string{"SELECT 1", "SELECT 2", "SELECT 3"})

	output := buf.String()
	assert.Contains(t, output, "Statements to execute:")
	assert.Contains(t, output, "1. SELECT 1")
	assert.Contains(t, output, "2. SELECT 2")
	assert.Contains(t, output, "3. SELECT 3")
}

func TestAskConfirmation(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		{"y", "y\n", true},
		{"Y", "Y\n", true},
		{"yes", "yes\n", true},
		{"YES", "YES\n", true},
		{"n", "n\n", false},
		{"no", "no\n", false},
		{"empty", "\n", false},
		{"random", "maybe\n", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			applier := NewApplier(Options{
				Out: &buf,
				In:  strings.NewReader(tt.input),
			})
			assert.Equal(t, tt.expected, applier.askConfirmation())
		})
	}
}
func TestApplierDisplayOutputIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	tc := setupMySQL(t)
	ctx := context.Background()

	t.Run("preflight display with database accessible", func(t *testing.T) {
		preflightWithDatabaseAccessibleTest(ctx, t, tc)
	})

	t.Run("preflight display shows warnings", func(t *testing.T) {
		preflightShowsWarningsTest(ctx, t, tc)
	})

	t.Run("preflight display shows non-transactional warnings", func(t *testing.T) {
		preflightNonTransactionalTest(ctx, t, tc)
	})

	t.Run("statement display shows numbered list", func(t *testing.T) {
		statementNumberedListTest(ctx, t, tc)
	})
}

func preflightWithDatabaseAccessibleTest(ctx context.Context, t *testing.T, tc *testMySQLContainer) {
	t.Helper()
	var buf bytes.Buffer
	applier := NewApplier(Options{
		DSN:    tc.dsn,
		DryRun: true,
		Out:    &buf,
	})
	require.NoError(t, applier.Connect(ctx))
	defer applier.Close()

	stmts := applier.ParseStatements("CREATE TABLE display_test (id INT PRIMARY KEY)")
	preflight := applier.PreflightChecks(stmts, false)
	_ = applier.Apply(ctx, stmts, preflight)

	output := buf.String()
	assert.Contains(t, output, "Database is accessible")
	assert.Contains(t, output, "All migrations are valid SQL")
	assert.Contains(t, output, "Statements to execute:")
}

func preflightShowsWarningsTest(ctx context.Context, t *testing.T, tc *testMySQLContainer) {
	t.Helper()
	var buf bytes.Buffer
	applier := NewApplier(Options{
		DSN:    tc.dsn,
		DryRun: true,
		Unsafe: true,
		Out:    &buf,
	})
	require.NoError(t, applier.Connect(ctx))
	defer applier.Close()

	stmts := []string{"DROP TABLE IF EXISTS some_table"}
	preflight := applier.PreflightChecks(stmts, true)
	_ = applier.Apply(ctx, stmts, preflight)

	output := buf.String()
	assert.Contains(t, output, "DANGER")
}

func preflightNonTransactionalTest(ctx context.Context, t *testing.T, tc *testMySQLContainer) {
	t.Helper()
	var buf bytes.Buffer
	applier := NewApplier(Options{
		DSN:    tc.dsn,
		DryRun: true,
		Out:    &buf,
	})
	require.NoError(t, applier.Connect(ctx))
	defer applier.Close()

	stmts := []string{"CREATE TABLE nonexistent_display (id INT PRIMARY KEY)"}
	preflight := applier.PreflightChecks(stmts, false)
	_ = applier.Apply(ctx, stmts, preflight)

	output := buf.String()
	assert.Contains(t, output, "NOT transaction-safe")
}

func statementNumberedListTest(ctx context.Context, t *testing.T, tc *testMySQLContainer) {
	t.Helper()
	var buf bytes.Buffer
	applier := NewApplier(Options{
		DSN:    tc.dsn,
		DryRun: true,
		Out:    &buf,
	})
	require.NoError(t, applier.Connect(ctx))
	defer applier.Close()

	stmts := []string{
		"CREATE TABLE numbered1 (id INT PRIMARY KEY)",
		"CREATE TABLE numbered2 (id INT PRIMARY KEY)",
		"CREATE TABLE numbered3 (id INT PRIMARY KEY)",
	}
	preflight := applier.PreflightChecks(stmts, false)
	_ = applier.Apply(ctx, stmts, preflight)

	output := buf.String()
	assert.Contains(t, output, "1.")
	assert.Contains(t, output, "2.")
	assert.Contains(t, output, "3.")
}
