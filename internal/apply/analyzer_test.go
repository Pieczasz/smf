package apply

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var analyzeStatementTests = []struct {
	name              string
	sql               string
	wantDestructive   bool
	wantBlocking      bool
	wantTxSafe        bool
	wantStatementType string
}{
	{
		name:              "DROP TABLE is destructive and non-transactional",
		sql:               "DROP TABLE users;",
		wantDestructive:   true,
		wantBlocking:      false,
		wantTxSafe:        false,
		wantStatementType: "DROP TABLE",
	},
	{
		name:              "DROP DATABASE is destructive and non-transactional",
		sql:               "DROP DATABASE mydb;",
		wantDestructive:   true,
		wantBlocking:      false,
		wantTxSafe:        false,
		wantStatementType: "DROP DATABASE",
	},
	{
		name:              "TRUNCATE TABLE is destructive and non-transactional",
		sql:               "TRUNCATE TABLE users;",
		wantDestructive:   true,
		wantBlocking:      true,
		wantTxSafe:        false,
		wantStatementType: "TRUNCATE TABLE",
	},
	{
		name:              "DELETE is destructive but transactional",
		sql:               "DELETE FROM users WHERE id = 1;",
		wantDestructive:   true,
		wantBlocking:      false,
		wantTxSafe:        true,
		wantStatementType: "DELETE",
	},
	{
		name:              "CREATE TABLE is non-transactional",
		sql:               "CREATE TABLE users (id INT PRIMARY KEY);",
		wantDestructive:   false,
		wantBlocking:      false,
		wantTxSafe:        false,
		wantStatementType: "CREATE TABLE",
	},
	{
		name:              "CREATE INDEX is blocking and non-transactional",
		sql:               "CREATE INDEX idx_name ON users(name);",
		wantDestructive:   false,
		wantBlocking:      true,
		wantTxSafe:        false,
		wantStatementType: "CREATE INDEX",
	},
	{
		name:              "CREATE DATABASE is non-transactional",
		sql:               "CREATE DATABASE mydb;",
		wantDestructive:   false,
		wantBlocking:      false,
		wantTxSafe:        false,
		wantStatementType: "CREATE DATABASE",
	},
	{
		name:              "ALTER TABLE ADD COLUMN is blocking",
		sql:               "ALTER TABLE users ADD COLUMN email VARCHAR(255);",
		wantDestructive:   false,
		wantBlocking:      true,
		wantTxSafe:        false,
		wantStatementType: "ALTER TABLE",
	},
	{
		name:              "ALTER TABLE DROP COLUMN is destructive and blocking",
		sql:               "ALTER TABLE users DROP COLUMN email;",
		wantDestructive:   true,
		wantBlocking:      true,
		wantTxSafe:        false,
		wantStatementType: "ALTER TABLE",
	},
	{
		name:              "RENAME TABLE is blocking and non-transactional",
		sql:               "RENAME TABLE old_users TO new_users;",
		wantDestructive:   false,
		wantBlocking:      true,
		wantTxSafe:        false,
		wantStatementType: "RENAME TABLE",
	},
	{
		name:              "DROP INDEX is blocking",
		sql:               "DROP INDEX idx_name ON users;",
		wantDestructive:   false,
		wantBlocking:      true,
		wantTxSafe:        false,
		wantStatementType: "DROP INDEX",
	},
}

func TestStatementAnalyzerAnalyzeStatement(t *testing.T) {
	analyzer := NewStatementAnalyzer()
	for _, tt := range analyzeStatementTests {
		t.Run(tt.name, func(t *testing.T) {
			analysis := analyzer.AnalyzeStatement(tt.sql)

			assert.Equal(t, tt.wantDestructive, analysis.IsDestructive, "IsDestructive mismatch")
			assert.Equal(t, tt.wantBlocking, analysis.IsBlocking, "IsBlocking mismatch")
			assert.Equal(t, tt.wantTxSafe, analysis.IsTransactionSafe, "IsTransactionSafe mismatch")
			if tt.wantStatementType != "" {
				assert.Equal(t, tt.wantStatementType, analysis.StatementType, "StatementType mismatch")
			}
		})
	}
}

func TestStatementAnalyzerPreflightResult(t *testing.T) {
	analyzer := NewStatementAnalyzer()

	statements := []string{
		"CREATE TABLE users (id INT PRIMARY KEY);",
		"ALTER TABLE users ADD COLUMN email VARCHAR(255);",
		"DROP TABLE old_users;",
	}

	result := analyzer.AnalyzeStatements(statements, false)

	assert.False(t, result.IsTransactional, "expected IsTransactional to be false for DDL statements")
	assert.NotEmpty(t, result.NonTxReasons, "expected NonTxReasons to be populated")
	assert.NotEmpty(t, result.Warnings, "expected Warnings to be populated")

	hasDanger := false
	for _, w := range result.Warnings {
		if w.Level == WarnDanger {
			hasDanger = true
			break
		}
	}
	assert.True(t, hasDanger, "expected at least one DANGER warning for DROP TABLE")
}

func TestStatementAnalyzerFalsePositiveAvoidance(t *testing.T) {
	analyzer := NewStatementAnalyzer()

	tests := []struct {
		name            string
		sql             string
		wantDestructive bool
	}{
		{
			name:            "String containing DROP TABLE should not be flagged",
			sql:             "INSERT INTO logs (message) VALUES ('User tried to DROP TABLE');",
			wantDestructive: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			analysis := analyzer.AnalyzeStatement(tt.sql)
			assert.Equal(t, tt.wantDestructive, analysis.IsDestructive, "false positive detected")
		})
	}
}

func TestStatementAnalyzerAlterTableSpecs(t *testing.T) {
	analyzer := NewStatementAnalyzer()

	tests := []struct {
		name            string
		sql             string
		wantBlocking    bool
		wantDestructive bool
	}{
		{
			name:            "ADD INDEX is blocking",
			sql:             "ALTER TABLE users ADD INDEX idx_name (name);",
			wantBlocking:    true,
			wantDestructive: false,
		},
		{
			name:            "DROP INDEX is blocking",
			sql:             "ALTER TABLE users DROP INDEX idx_name;",
			wantBlocking:    true,
			wantDestructive: false,
		},
		{
			name:            "ADD FOREIGN KEY is blocking",
			sql:             "ALTER TABLE orders ADD CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id);",
			wantBlocking:    true,
			wantDestructive: false,
		},
		{
			name:            "DROP PRIMARY KEY is blocking",
			sql:             "ALTER TABLE users DROP PRIMARY KEY;",
			wantBlocking:    true,
			wantDestructive: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			analysis := analyzer.AnalyzeStatement(tt.sql)
			assert.Equal(t, tt.wantBlocking, analysis.IsBlocking, "IsBlocking mismatch")
			assert.Equal(t, tt.wantDestructive, analysis.IsDestructive, "IsDestructive mismatch")
		})
	}
}
