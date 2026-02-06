package apply

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestApplierPreflightChecks(t *testing.T) {
	applier := NewApplier(Options{})

	t.Run("safe DDL statements", func(t *testing.T) {
		stmts := []string{
			"CREATE TABLE users (id INT PRIMARY KEY)",
			"ALTER TABLE users ADD COLUMN name VARCHAR(255)",
		}
		result := applier.PreflightChecks(stmts, false)
		assert.False(t, result.IsTransactional)
		assert.False(t, result.HasDestructiveOperations())
	})

	t.Run("destructive statements detected", func(t *testing.T) {
		stmts := []string{
			"DROP TABLE users",
		}
		result := applier.PreflightChecks(stmts, false)
		assert.True(t, result.HasDestructiveOperations())
	})

	t.Run("unsafe flag suppresses requires-unsafe tag", func(t *testing.T) {
		stmts := []string{
			"DROP TABLE users",
		}
		resultUnsafe := applier.PreflightChecks(stmts, true)
		resultSafe := applier.PreflightChecks(stmts, false)

		assert.True(t, resultUnsafe.HasDestructiveOperations())
		assert.True(t, resultSafe.HasDestructiveOperations())

		for _, w := range resultSafe.Warnings {
			if w.Level == WarnDanger {
				assert.Contains(t, w.Message, "--unsafe")
			}
		}
		for _, w := range resultUnsafe.Warnings {
			if w.Level == WarnDanger {
				assert.NotContains(t, w.Message, "--unsafe")
			}
		}
	})

	t.Run("transaction-safe DML", func(t *testing.T) {
		stmts := []string{
			"INSERT INTO users (name) VALUES ('Alice')",
			"UPDATE users SET name = 'Bob' WHERE id = 1",
		}
		result := applier.PreflightChecks(stmts, false)
		assert.True(t, result.IsTransactional)
	})

	t.Run("non-transactional DDL with reasons", func(t *testing.T) {
		stmts := []string{
			"CREATE TABLE t (id INT PRIMARY KEY)",
			"DROP TABLE t",
		}
		result := applier.PreflightChecks(stmts, true)
		assert.False(t, result.IsTransactional)
		assert.NotEmpty(t, result.NonTxReasons)
	})

	t.Run("empty statements produce clean result", func(t *testing.T) {
		result := applier.PreflightChecks([]string{}, false)
		assert.True(t, result.IsTransactional)
		assert.Empty(t, result.Warnings)
		assert.Empty(t, result.Errors)
	})
}

func TestApplierValidatePreflight(t *testing.T) {
	t.Run("destructive without unsafe flag fails", func(t *testing.T) {
		applier := NewApplier(Options{Unsafe: false})
		preflight := &PreflightResult{
			Warnings: []Warning{
				{Level: WarnDanger, Message: "destructive op"},
			},
		}
		err := applier.validatePreflight(preflight)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "destructive")
	})

	t.Run("destructive with unsafe flag passes", func(t *testing.T) {
		applier := NewApplier(Options{Unsafe: true})
		preflight := &PreflightResult{
			Warnings: []Warning{
				{Level: WarnDanger, Message: "destructive op"},
			},
			IsTransactional: true,
		}
		err := applier.validatePreflight(preflight)
		assert.NoError(t, err)
	})

	t.Run("non-transactional without allow flag and tx mode fails", func(t *testing.T) {
		applier := NewApplier(Options{
			Transaction:           true,
			AllowNonTransactional: false,
		})
		preflight := &PreflightResult{
			IsTransactional: false,
		}
		err := applier.validatePreflight(preflight)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "non-transactional")
	})

	t.Run("non-transactional with allow flag passes", func(t *testing.T) {
		applier := NewApplier(Options{
			Transaction:           true,
			AllowNonTransactional: true,
		})
		preflight := &PreflightResult{
			IsTransactional: false,
		}
		err := applier.validatePreflight(preflight)
		assert.NoError(t, err)
	})

	t.Run("caution warnings do not block", func(t *testing.T) {
		applier := NewApplier(Options{Unsafe: false})
		preflight := &PreflightResult{
			Warnings: []Warning{
				{Level: WarnCaution, Message: "blocking DDL"},
			},
			IsTransactional: true,
		}
		err := applier.validatePreflight(preflight)
		assert.NoError(t, err)
	})
}

func TestHasDestructiveOperations(t *testing.T) {
	t.Run("no warnings", func(t *testing.T) {
		assert.False(t, (&PreflightResult{}).HasDestructiveOperations())
	})

	t.Run("only caution", func(t *testing.T) {
		assert.False(t, (&PreflightResult{
			Warnings: []Warning{{Level: WarnCaution}},
		}).HasDestructiveOperations())
	})

	t.Run("has danger", func(t *testing.T) {
		assert.True(t, (&PreflightResult{
			Warnings: []Warning{
				{Level: WarnCaution},
				{Level: WarnDanger},
			},
		}).HasDestructiveOperations())
	})
}

func TestWarningLevelConstants(t *testing.T) {
	assert.Equal(t, WarningLevel("CAUTION"), WarnCaution)
	assert.Equal(t, WarningLevel("DANGER"), WarnDanger)
}

func TestPreflightResultStructure(t *testing.T) {
	pr := &PreflightResult{
		Warnings:        []Warning{{Level: WarnCaution, Message: "test", SQL: "SELECT 1"}},
		Errors:          []string{"err1"},
		IsTransactional: false,
		NonTxReasons:    []string{"reason1"},
	}
	assert.Len(t, pr.Warnings, 1)
	assert.Len(t, pr.Errors, 1)
	assert.False(t, pr.IsTransactional)
	assert.Len(t, pr.NonTxReasons, 1)
}

func TestAnalyzerOtherStatements(t *testing.T) {
	t.Run("CREATE VIEW is non-transactional", func(t *testing.T) {
		testCreateView(t)
	})

	t.Run("ALTER DATABASE is non-transactional", func(t *testing.T) {
		testAlterDatabase(t)
	})

	t.Run("DML statements", func(t *testing.T) {
		testDMLStatements(t)
	})

	t.Run("Procedure and trigger statements", func(t *testing.T) {
		testProcedureTriggers(t)
	})

	t.Run("ALTER TABLE blocking operations", func(t *testing.T) {
		testAlterTableBlocking(t)
	})
}

func TestAnalyzerAddTransactionSafetyWithEmptyReason(t *testing.T) {
	analyzer := NewStatementAnalyzer()

	result := &PreflightResult{IsTransactional: true}
	analysis := &StatementAnalysis{
		IsTransactionSafe: false,
		TxUnsafeReason:    "",
	}
	analyzer.addTransactionSafety(result, analysis, "CREATE TABLE foo (id INT)")
	assert.False(t, result.IsTransactional)
	assert.Contains(t, result.NonTxReasons[0], "DDL statement causes implicit commit")
}

func TestAnalyzerAddTransactionSafetyWithReason(t *testing.T) {
	analyzer := NewStatementAnalyzer()

	result := &PreflightResult{IsTransactional: true}
	analysis := &StatementAnalysis{
		IsTransactionSafe: false,
		TxUnsafeReason:    "specific reason",
	}
	analyzer.addTransactionSafety(result, analysis, "ALTER TABLE t ADD COLUMN x INT")
	assert.False(t, result.IsTransactional)
	assert.Contains(t, result.NonTxReasons[0], "specific reason")
}

func TestAnalyzerNilAnalysis(t *testing.T) {
	analyzer := NewStatementAnalyzer()

	result := analyzer.AnalyzeStatements([]string{"SELECT 1"}, false)
	assert.True(t, result.IsTransactional)
}

func testCreateView(t *testing.T) {
	t.Helper()
	analyzer := NewStatementAnalyzer()
	analysis := analyzer.AnalyzeStatement("CREATE VIEW v AS SELECT 1")
	assert.Equal(t, "CREATE VIEW", analysis.StatementType)
	assert.False(t, analysis.IsTransactionSafe)
}

func testAlterDatabase(t *testing.T) {
	t.Helper()
	analyzer := NewStatementAnalyzer()
	analysis := analyzer.AnalyzeStatement("ALTER DATABASE testdb CHARACTER SET utf8mb4")
	assert.Equal(t, "ALTER DATABASE", analysis.StatementType)
	assert.False(t, analysis.IsTransactionSafe)
}

func testDMLStatements(t *testing.T) {
	t.Helper()
	analyzer := NewStatementAnalyzer()

	analysis := analyzer.AnalyzeStatement("INSERT INTO t VALUES (1)")
	assert.True(t, analysis.IsTransactionSafe)

	analysis = analyzer.AnalyzeStatement("UPDATE t SET x = 1")
	assert.True(t, analysis.IsTransactionSafe)

	analysis = analyzer.AnalyzeStatement("SELECT * FROM t")
	assert.True(t, analysis.IsTransactionSafe)
}

func testProcedureTriggers(t *testing.T) {
	t.Helper()
	analyzer := NewStatementAnalyzer()

	analysis := analyzer.AnalyzeStatement("CREATE PROCEDURE p() BEGIN SELECT 1; END")
	assert.False(t, analysis.IsTransactionSafe)

	analysis = analyzer.AnalyzeStatement("DROP PROCEDURE IF EXISTS myproc")
	assert.False(t, analysis.IsTransactionSafe)

	analysis = analyzer.AnalyzeStatement("CREATE TRIGGER trg BEFORE INSERT ON t FOR EACH ROW SET @x = 1")
	assert.False(t, analysis.IsTransactionSafe)

	analysis = analyzer.AnalyzeStatement("DROP TRIGGER IF EXISTS trg")
	assert.False(t, analysis.IsTransactionSafe)

	analysis = analyzer.AnalyzeStatement("CREATE EVENT ev ON SCHEDULE EVERY 1 HOUR DO SELECT 1")
	assert.False(t, analysis.IsTransactionSafe)

	analysis = analyzer.AnalyzeStatement("DROP EVENT IF EXISTS ev")
	assert.False(t, analysis.IsTransactionSafe)
}

func testAlterTableBlocking(t *testing.T) {
	t.Helper()
	analyzer := NewStatementAnalyzer()

	analysis := analyzer.AnalyzeStatement("ALTER TABLE t MODIFY COLUMN x BIGINT")
	assert.True(t, analysis.IsBlocking)

	analysis = analyzer.AnalyzeStatement("ALTER TABLE t CHANGE COLUMN old_name new_name INT")
	assert.True(t, analysis.IsBlocking)

	analysis = analyzer.AnalyzeStatement("ALTER TABLE t DROP FOREIGN KEY fk_name")
	assert.True(t, analysis.IsBlocking)

	analysis = analyzer.AnalyzeStatement("ALTER TABLE t RENAME TO t2")
	assert.True(t, analysis.IsBlocking)

	analysis = analyzer.AnalyzeStatement("ALTER TABLE t ADD UNIQUE INDEX idx_unique (col)")
	assert.True(t, analysis.IsBlocking)

	analysis = analyzer.AnalyzeStatement("ALTER TABLE t ADD CONSTRAINT chk CHECK (col > 0)")
	assert.True(t, analysis.IsBlocking)

	analysis = analyzer.AnalyzeStatement("ALTER TABLE t FORCE")
	assert.True(t, analysis.IsBlocking)
}
