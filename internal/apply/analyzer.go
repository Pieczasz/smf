package apply

import (
	"fmt"
	"strings"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
)

// StatementAnalysis contains the results of analyzing a SQL statement.
type StatementAnalysis struct {
	IsBlocking        bool
	BlockingReasons   []string
	IsDestructive     bool
	DestructiveReason string
	IsTransactionSafe bool
	TxUnsafeReason    string
	StatementType     string
}

// StatementAnalyzer uses TiDB's AST parser for reliable SQL analysis
type StatementAnalyzer struct {
	parser *parser.Parser
}

// NewStatementAnalyzer creates a new AST-based statement analyzer.
func NewStatementAnalyzer() *StatementAnalyzer {
	return &StatementAnalyzer{
		parser: parser.New(),
	}
}

// AnalyzeStatement parses a single SQL statement and returns analysis results.
func (a *StatementAnalyzer) AnalyzeStatement(sql string) (*StatementAnalysis, error) {
	stmtNodes, _, err := a.parser.Parse(sql, "", "")
	if err != nil {
		return a.fallbackAnalysis(sql), nil
	}

	if len(stmtNodes) == 0 {
		return &StatementAnalysis{}, nil
	}

	return a.analyzeNode(stmtNodes[0], sql), nil
}

// AnalyzeStatements analyzes multiple SQL statements and returns a PreflightResult.
func (a *StatementAnalyzer) AnalyzeStatements(statements []string, unsafeAllowed bool) *PreflightResult {
	result := &PreflightResult{
		IsTransactional: true,
	}

	for _, stmt := range statements {
		analysis, _ := a.AnalyzeStatement(stmt)
		if analysis == nil {
			continue
		}

		if analysis.IsBlocking {
			for _, reason := range analysis.BlockingReasons {
				result.Warnings = append(result.Warnings, Warning{
					Level:   WarnCaution,
					Message: fmt.Sprintf("Potentially blocking DDL: %s", reason),
					SQL:     truncateSQL(stmt),
				})
			}
		}

		if analysis.IsDestructive {
			msg := analysis.DestructiveReason
			if !unsafeAllowed {
				msg = fmt.Sprintf("%s (requires --unsafe flag)", msg)
			}
			result.Warnings = append(result.Warnings, Warning{
				Level:   WarnDanger,
				Message: msg,
				SQL:     truncateSQL(stmt),
			})
		}

		if !analysis.IsTransactionSafe {
			result.IsTransactional = false
			reason := analysis.TxUnsafeReason
			if reason != "" {
				reason = fmt.Sprintf("%s: %s", reason, truncateSQL(stmt))
			} else {
				reason = fmt.Sprintf("DDL statement causes implicit commit: %s", truncateSQL(stmt))
			}
			result.NonTxReasons = append(result.NonTxReasons, reason)
		}
	}

	return result
}

func (a *StatementAnalyzer) analyzeNode(node ast.StmtNode, originalSQL string) *StatementAnalysis {
	analysis := &StatementAnalysis{
		IsTransactionSafe: true,
	}

	switch stmt := node.(type) {
	case *ast.DropTableStmt:
		analysis.StatementType = "DROP TABLE"
		analysis.IsDestructive = true
		analysis.DestructiveReason = "DROP TABLE will permanently delete the table and all its data"
		analysis.IsTransactionSafe = false
		analysis.TxUnsafeReason = "DROP TABLE causes an implicit commit in MySQL"

	case *ast.DropDatabaseStmt:
		analysis.StatementType = "DROP DATABASE"
		analysis.IsDestructive = true
		analysis.DestructiveReason = "DROP DATABASE will permanently delete the entire database"
		analysis.IsTransactionSafe = false
		analysis.TxUnsafeReason = "DROP DATABASE causes an implicit commit in MySQL"

	case *ast.DropIndexStmt:
		analysis.StatementType = "DROP INDEX"
		analysis.IsBlocking = true
		analysis.BlockingReasons = append(analysis.BlockingReasons, "DROP INDEX may briefly lock the table")
		analysis.IsTransactionSafe = false
		analysis.TxUnsafeReason = "DROP INDEX causes an implicit commit in MySQL"

	case *ast.CreateTableStmt:
		analysis.StatementType = "CREATE TABLE"
		analysis.IsTransactionSafe = false
		analysis.TxUnsafeReason = "CREATE TABLE causes an implicit commit in MySQL"

	case *ast.CreateDatabaseStmt:
		analysis.StatementType = "CREATE DATABASE"
		analysis.IsTransactionSafe = false
		analysis.TxUnsafeReason = "CREATE DATABASE causes an implicit commit in MySQL"

	case *ast.CreateIndexStmt:
		analysis.StatementType = "CREATE INDEX"
		analysis.IsBlocking = true
		analysis.BlockingReasons = append(analysis.BlockingReasons, "CREATE INDEX may lock the table for the duration of index creation")
		analysis.IsTransactionSafe = false
		analysis.TxUnsafeReason = "CREATE INDEX causes an implicit commit in MySQL"

	case *ast.CreateViewStmt:
		analysis.StatementType = "CREATE VIEW"
		analysis.IsTransactionSafe = false
		analysis.TxUnsafeReason = "CREATE VIEW causes an implicit commit in MySQL"

	case *ast.AlterTableStmt:
		analysis.StatementType = "ALTER TABLE"
		analysis.IsTransactionSafe = false
		analysis.TxUnsafeReason = "ALTER TABLE causes an implicit commit in MySQL"
		a.analyzeAlterTable(stmt, analysis)

	case *ast.AlterDatabaseStmt:
		analysis.StatementType = "ALTER DATABASE"
		analysis.IsTransactionSafe = false
		analysis.TxUnsafeReason = "ALTER DATABASE causes an implicit commit in MySQL"

	case *ast.RenameTableStmt:
		analysis.StatementType = "RENAME TABLE"
		analysis.IsBlocking = true
		analysis.BlockingReasons = append(analysis.BlockingReasons, "RENAME TABLE acquires an exclusive lock but is typically fast")
		analysis.IsTransactionSafe = false
		analysis.TxUnsafeReason = "RENAME TABLE causes an implicit commit in MySQL"

	case *ast.TruncateTableStmt:
		analysis.StatementType = "TRUNCATE TABLE"
		analysis.IsDestructive = true
		analysis.DestructiveReason = "TRUNCATE TABLE will delete all rows from the table"
		analysis.IsBlocking = true
		analysis.BlockingReasons = append(analysis.BlockingReasons, "TRUNCATE TABLE acquires an exclusive lock and removes all data instantly")
		analysis.IsTransactionSafe = false
		analysis.TxUnsafeReason = "TRUNCATE TABLE causes an implicit commit in MySQL"

	case *ast.DeleteStmt:
		analysis.StatementType = "DELETE"
		analysis.IsDestructive = true
		analysis.DestructiveReason = "DELETE will remove rows from the table"

	case *ast.InsertStmt, *ast.UpdateStmt, *ast.SelectStmt:
		analysis.IsTransactionSafe = true
		// TODO: Add support for all possible cases
	default:
		analysis.StatementType = "OTHER"
		upper := strings.ToUpper(strings.TrimSpace(originalSQL))

		ddlKeywords := map[string]struct {
			txReason string
		}{
			"CREATE VIEW":      {"CREATE VIEW causes an implicit commit in MySQL"},
			"DROP VIEW":        {"DROP VIEW causes an implicit commit in MySQL"},
			"ALTER VIEW":       {"ALTER VIEW causes an implicit commit in MySQL"},
			"CREATE FUNCTION":  {"CREATE FUNCTION causes an implicit commit in MySQL"},
			"DROP FUNCTION":    {"DROP FUNCTION causes an implicit commit in MySQL"},
			"ALTER FUNCTION":   {"ALTER FUNCTION causes an implicit commit in MySQL"},
			"CREATE PROCEDURE": {"CREATE PROCEDURE causes an implicit commit in MySQL"},
			"DROP PROCEDURE":   {"DROP PROCEDURE causes an implicit commit in MySQL"},
			"ALTER PROCEDURE":  {"ALTER PROCEDURE causes an implicit commit in MySQL"},
			"CREATE TRIGGER":   {"CREATE TRIGGER causes an implicit commit in MySQL"},
			"DROP TRIGGER":     {"DROP TRIGGER causes an implicit commit in MySQL"},
			"CREATE EVENT":     {"CREATE EVENT causes an implicit commit in MySQL"},
			"DROP EVENT":       {"DROP EVENT causes an implicit commit in MySQL"},
			"ALTER EVENT":      {"ALTER EVENT causes an implicit commit in MySQL"},
			"CREATE SEQUENCE":  {"CREATE SEQUENCE causes an implicit commit"},
			"DROP SEQUENCE":    {"DROP SEQUENCE causes an implicit commit"},
		}

		for keyword, info := range ddlKeywords {
			if strings.HasPrefix(upper, keyword) {
				analysis.StatementType = keyword
				analysis.IsTransactionSafe = false
				analysis.TxUnsafeReason = info.txReason
				break
			}
		}

		if analysis.IsTransactionSafe {
			if strings.HasPrefix(upper, "CREATE ") ||
				strings.HasPrefix(upper, "DROP ") ||
				strings.HasPrefix(upper, "ALTER ") {
				analysis.IsTransactionSafe = false
				analysis.TxUnsafeReason = "DDL statement causes implicit commit"
			}
		}
	}

	return analysis
}

func (a *StatementAnalyzer) analyzeAlterTable(stmt *ast.AlterTableStmt, analysis *StatementAnalysis) {
	for _, spec := range stmt.Specs {
		switch spec.Tp {
		case ast.AlterTableAddColumns:
			analysis.IsBlocking = true
			analysis.BlockingReasons = append(analysis.BlockingReasons,
				"ADD COLUMN may require a table rebuild depending on MySQL version and column position")

		case ast.AlterTableDropColumn:
			analysis.IsBlocking = true
			analysis.IsDestructive = true
			analysis.DestructiveReason = "DROP COLUMN will permanently delete the column and its data"
			analysis.BlockingReasons = append(analysis.BlockingReasons,
				"DROP COLUMN typically requires a full table rebuild and will lock the table")

		case ast.AlterTableModifyColumn, ast.AlterTableChangeColumn:
			analysis.IsBlocking = true
			if spec.Tp == ast.AlterTableModifyColumn {
				analysis.BlockingReasons = append(analysis.BlockingReasons,
					"MODIFY COLUMN may require a table rebuild if changing column type or size")
			} else {
				analysis.BlockingReasons = append(analysis.BlockingReasons,
					"CHANGE COLUMN may require a table rebuild")
			}

		case ast.AlterTableAddConstraint:
			analysis.IsBlocking = true
			if spec.Constraint != nil {
				switch spec.Constraint.Tp {
				case ast.ConstraintForeignKey:
					analysis.BlockingReasons = append(analysis.BlockingReasons,
						"ADD FOREIGN KEY may lock the table while validating existing data")
				case ast.ConstraintIndex, ast.ConstraintKey, ast.ConstraintUniq, ast.ConstraintUniqKey, ast.ConstraintUniqIndex:
					analysis.BlockingReasons = append(analysis.BlockingReasons,
						"ADD INDEX may lock the table for the duration of index creation on large tables")
				default:
					analysis.BlockingReasons = append(analysis.BlockingReasons,
						"ADD CONSTRAINT may lock the table while validating existing data")
				}
			}

		case ast.AlterTableDropIndex:
			analysis.IsBlocking = true
			analysis.BlockingReasons = append(analysis.BlockingReasons,
				"DROP INDEX may briefly lock the table")

		case ast.AlterTableDropForeignKey:
			analysis.IsBlocking = true
			analysis.BlockingReasons = append(analysis.BlockingReasons,
				"DROP FOREIGN KEY may briefly lock the table")

		case ast.AlterTableDropPrimaryKey:
			analysis.IsBlocking = true
			analysis.BlockingReasons = append(analysis.BlockingReasons,
				"DROP PRIMARY KEY requires a full table rebuild and will lock the table")

		case ast.AlterTableRenameTable:
			analysis.IsBlocking = true
			analysis.BlockingReasons = append(analysis.BlockingReasons,
				"RENAME TABLE acquires an exclusive lock but is typically fast")
		}
		// TODO: Add support for all possible cases
	}
}

func (a *StatementAnalyzer) fallbackAnalysis(sql string) *StatementAnalysis {
	analysis := &StatementAnalysis{
		StatementType:     "UNPARSEABLE",
		IsTransactionSafe: true,
	}

	upper := strings.ToUpper(strings.TrimSpace(sql))

	destructivePatterns := map[string]string{
		"DROP TABLE":     "DROP TABLE will permanently delete the table and all its data",
		"DROP DATABASE":  "DROP DATABASE will permanently delete the entire database",
		"TRUNCATE TABLE": "TRUNCATE TABLE will delete all rows from the table",
		"DELETE FROM":    "DELETE will remove rows from the table",
	}

	for pattern, reason := range destructivePatterns {
		if strings.Contains(upper, pattern) {
			analysis.IsDestructive = true
			analysis.DestructiveReason = reason
			break
		}
	}

	ddlPrefixes := []string{
		"CREATE TABLE", "DROP TABLE", "ALTER TABLE", "RENAME TABLE", "TRUNCATE TABLE",
		"CREATE INDEX", "DROP INDEX",
		"CREATE DATABASE", "DROP DATABASE", "ALTER DATABASE",
		"CREATE VIEW", "DROP VIEW", "ALTER VIEW",
		"CREATE PROCEDURE", "DROP PROCEDURE", "ALTER PROCEDURE",
		"CREATE FUNCTION", "DROP FUNCTION", "ALTER FUNCTION",
		"CREATE TRIGGER", "DROP TRIGGER",
		"CREATE EVENT", "DROP EVENT", "ALTER EVENT",
	}

	for _, prefix := range ddlPrefixes {
		if strings.HasPrefix(upper, prefix) || strings.Contains(upper, " "+prefix) {
			analysis.IsTransactionSafe = false
			analysis.TxUnsafeReason = fmt.Sprintf("%s causes an implicit commit in MySQL", prefix)
			break
		}
	}

	if strings.Contains(upper, "ALTER TABLE") && strings.Contains(upper, "DROP COLUMN") {
		analysis.IsDestructive = true
		analysis.DestructiveReason = "DROP COLUMN will permanently delete the column and its data"
	}

	return analysis
}
