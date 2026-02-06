package apply

import (
	"fmt"
	"strings"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver" // required to register TiDB parser driver implementations
)

var ddlImplicitCommitReasons = map[string]string{
	"CREATE VIEW":      "CREATE VIEW causes an implicit commit in MySQL",
	"DROP VIEW":        "DROP VIEW causes an implicit commit in MySQL",
	"ALTER VIEW":       "ALTER VIEW causes an implicit commit in MySQL",
	"CREATE FUNCTION":  "CREATE FUNCTION causes an implicit commit in MySQL",
	"DROP FUNCTION":    "DROP FUNCTION causes an implicit commit in MySQL",
	"ALTER FUNCTION":   "ALTER FUNCTION causes an implicit commit in MySQL",
	"CREATE PROCEDURE": "CREATE PROCEDURE causes an implicit commit in MySQL",
	"DROP PROCEDURE":   "DROP PROCEDURE causes an implicit commit in MySQL",
	"ALTER PROCEDURE":  "ALTER PROCEDURE causes an implicit commit in MySQL",
	"CREATE TRIGGER":   "CREATE TRIGGER causes an implicit commit in MySQL",
	"DROP TRIGGER":     "DROP TRIGGER causes an implicit commit in MySQL",
	"CREATE EVENT":     "CREATE EVENT causes an implicit commit in MySQL",
	"DROP EVENT":       "DROP EVENT causes an implicit commit in MySQL",
	"ALTER EVENT":      "ALTER EVENT causes an implicit commit in MySQL",
	"CREATE SEQUENCE":  "CREATE SEQUENCE causes an implicit commit",
	"DROP SEQUENCE":    "DROP SEQUENCE causes an implicit commit",
}

type alterTableSpecEffect struct {
	blocking          bool
	destructive       bool
	destructiveReason string
	blockingReason    string
}

var alterTableSpecEffects = map[ast.AlterTableType]alterTableSpecEffect{
	ast.AlterTableAddColumns: {
		blocking:       true,
		blockingReason: "ADD COLUMN may require a table rebuild depending on MySQL version and column position",
	},
	ast.AlterTableDropColumn: {
		blocking:          true,
		destructive:       true,
		destructiveReason: "DROP COLUMN will permanently delete the column and its data",
		blockingReason:    "DROP COLUMN typically requires a full table rebuild and will lock the table",
	},
	ast.AlterTableModifyColumn: {
		blocking:       true,
		blockingReason: "MODIFY COLUMN may require a table rebuild if changing column type or size",
	},
	ast.AlterTableChangeColumn: {
		blocking:       true,
		blockingReason: "CHANGE COLUMN may require a table rebuild",
	},
	ast.AlterTableDropIndex: {
		blocking:       true,
		blockingReason: "DROP INDEX may briefly lock the table",
	},
	ast.AlterTableDropForeignKey: {
		blocking:       true,
		blockingReason: "DROP FOREIGN KEY may briefly lock the table",
	},
	ast.AlterTableDropPrimaryKey: {
		blocking:       true,
		blockingReason: "DROP PRIMARY KEY requires a full table rebuild and will lock the table",
	},
	ast.AlterTableRenameTable: {
		blocking:       true,
		blockingReason: "RENAME TABLE acquires an exclusive lock but is typically fast",
	},
	ast.AlterTableForce: {
		blocking:       true,
		blockingReason: "FORCE rebuilds the table and will lock it",
	},
}

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
func (a *StatementAnalyzer) AnalyzeStatement(sql string) *StatementAnalysis {
	stmtNodes, _, err := a.parser.Parse(sql, "", "")
	if err != nil {
		analysis := &StatementAnalysis{
			StatementType:     "UNPARSEABLE",
			IsTransactionSafe: true,
		}
		a.analyzeOtherStatement(analysis, sql)
		return analysis
	}

	if len(stmtNodes) == 0 {
		return &StatementAnalysis{}
	}

	return a.analyzeNode(stmtNodes[0], sql)
}

// AnalyzeStatements analyzes multiple SQL statements and returns a PreflightResult.
func (a *StatementAnalyzer) AnalyzeStatements(statements []string, unsafeAllowed bool) *PreflightResult {
	result := &PreflightResult{
		IsTransactional: true,
	}

	for _, stmt := range statements {
		analysis := a.AnalyzeStatement(stmt)
		if analysis == nil {
			continue
		}

		a.addBlockingWarnings(result, analysis, stmt)
		a.addDestructiveWarning(result, analysis, stmt, unsafeAllowed)
		a.addTransactionSafety(result, analysis, stmt)
	}

	return result
}

func (a *StatementAnalyzer) addBlockingWarnings(result *PreflightResult, analysis *StatementAnalysis, stmt string) {
	if !analysis.IsBlocking {
		return
	}
	for _, reason := range analysis.BlockingReasons {
		result.Warnings = append(result.Warnings, Warning{
			Level:   WarnCaution,
			Message: fmt.Sprintf("Potentially blocking DDL: %s", reason),
			SQL:     stmt,
		})
	}
}

func (a *StatementAnalyzer) addDestructiveWarning(result *PreflightResult, analysis *StatementAnalysis, stmt string, unsafeAllowed bool) {
	if !analysis.IsDestructive {
		return
	}
	msg := analysis.DestructiveReason
	if !unsafeAllowed {
		msg = fmt.Sprintf("%s (requires --unsafe flag)", msg)
	}
	result.Warnings = append(result.Warnings, Warning{
		Level:   WarnDanger,
		Message: msg,
		SQL:     stmt,
	})
}

func (a *StatementAnalyzer) addTransactionSafety(result *PreflightResult, analysis *StatementAnalysis, stmt string) {
	if analysis.IsTransactionSafe {
		return
	}
	result.IsTransactional = false
	reason := analysis.TxUnsafeReason
	if reason != "" {
		reason = fmt.Sprintf("%s: %s", reason, stmt)
	} else {
		reason = fmt.Sprintf("DDL statement causes implicit commit: %s", stmt)
	}
	result.NonTxReasons = append(result.NonTxReasons, reason)
}

func (a *StatementAnalyzer) analyzeNode(node ast.StmtNode, originalSQL string) *StatementAnalysis {
	analysis := &StatementAnalysis{
		IsTransactionSafe: true,
	}

	if a.analyzeDropNode(node, analysis) {
		return analysis
	}
	if a.analyzeCreateNode(node, analysis) {
		return analysis
	}
	if a.analyzeAlterNode(node, analysis) {
		return analysis
	}
	if a.analyzeRenameNode(node, analysis) {
		return analysis
	}
	if a.analyzeTruncateNode(node, analysis) {
		return analysis
	}
	if a.analyzeDMLNode(node, analysis) {
		return analysis
	}

	a.analyzeOtherStatement(analysis, originalSQL)

	return analysis
}

func (a *StatementAnalyzer) analyzeDropNode(node ast.StmtNode, analysis *StatementAnalysis) bool {
	switch node.(type) {
	case *ast.DropTableStmt:
		analysis.StatementType = "DROP TABLE"
		analysis.IsDestructive = true
		analysis.DestructiveReason = "DROP TABLE will permanently delete the table and all its data"
		analysis.IsTransactionSafe = false
		analysis.TxUnsafeReason = "DROP TABLE causes an implicit commit in MySQL"
		return true
	case *ast.DropDatabaseStmt:
		analysis.StatementType = "DROP DATABASE"
		analysis.IsDestructive = true
		analysis.DestructiveReason = "DROP DATABASE will permanently delete the entire database"
		analysis.IsTransactionSafe = false
		analysis.TxUnsafeReason = "DROP DATABASE causes an implicit commit in MySQL"
		return true
	case *ast.DropIndexStmt:
		analysis.StatementType = "DROP INDEX"
		analysis.IsBlocking = true
		analysis.BlockingReasons = append(analysis.BlockingReasons, "DROP INDEX may briefly lock the table")
		analysis.IsTransactionSafe = false
		analysis.TxUnsafeReason = "DROP INDEX causes an implicit commit in MySQL"
		return true
	default:
		return false
	}
}

func (a *StatementAnalyzer) analyzeCreateNode(node ast.StmtNode, analysis *StatementAnalysis) bool {
	switch node.(type) {
	case *ast.CreateTableStmt:
		analysis.StatementType = "CREATE TABLE"
		analysis.IsTransactionSafe = false
		analysis.TxUnsafeReason = "CREATE TABLE causes an implicit commit in MySQL"
		return true
	case *ast.CreateDatabaseStmt:
		analysis.StatementType = "CREATE DATABASE"
		analysis.IsTransactionSafe = false
		analysis.TxUnsafeReason = "CREATE DATABASE causes an implicit commit in MySQL"
		return true
	case *ast.CreateIndexStmt:
		analysis.StatementType = "CREATE INDEX"
		analysis.IsBlocking = true
		analysis.BlockingReasons = append(analysis.BlockingReasons, "CREATE INDEX may lock the table for the duration of index creation")
		analysis.IsTransactionSafe = false
		analysis.TxUnsafeReason = "CREATE INDEX causes an implicit commit in MySQL"
		return true
	case *ast.CreateViewStmt:
		analysis.StatementType = "CREATE VIEW"
		analysis.IsTransactionSafe = false
		analysis.TxUnsafeReason = "CREATE VIEW causes an implicit commit in MySQL"
		return true
	default:
		return false
	}
}

func (a *StatementAnalyzer) analyzeAlterNode(node ast.StmtNode, analysis *StatementAnalysis) bool {
	switch stmt := node.(type) {
	case *ast.AlterTableStmt:
		analysis.StatementType = "ALTER TABLE"
		analysis.IsTransactionSafe = false
		analysis.TxUnsafeReason = "ALTER TABLE causes an implicit commit in MySQL"
		a.analyzeAlterTable(stmt, analysis)
		return true
	case *ast.AlterDatabaseStmt:
		analysis.StatementType = "ALTER DATABASE"
		analysis.IsTransactionSafe = false
		analysis.TxUnsafeReason = "ALTER DATABASE causes an implicit commit in MySQL"
		return true
	default:
		return false
	}
}

func (a *StatementAnalyzer) analyzeRenameNode(node ast.StmtNode, analysis *StatementAnalysis) bool {
	if _, ok := node.(*ast.RenameTableStmt); !ok {
		return false
	}
	analysis.StatementType = "RENAME TABLE"
	analysis.IsBlocking = true
	analysis.BlockingReasons = append(analysis.BlockingReasons, "RENAME TABLE acquires an exclusive lock but is typically fast")
	analysis.IsTransactionSafe = false
	analysis.TxUnsafeReason = "RENAME TABLE causes an implicit commit in MySQL"
	return true
}

func (a *StatementAnalyzer) analyzeTruncateNode(node ast.StmtNode, analysis *StatementAnalysis) bool {
	if _, ok := node.(*ast.TruncateTableStmt); !ok {
		return false
	}
	analysis.StatementType = "TRUNCATE TABLE"
	analysis.IsDestructive = true
	analysis.DestructiveReason = "TRUNCATE TABLE will delete all rows from the table"
	analysis.IsBlocking = true
	analysis.BlockingReasons = append(analysis.BlockingReasons, "TRUNCATE TABLE acquires an exclusive lock and removes all data instantly")
	analysis.IsTransactionSafe = false
	analysis.TxUnsafeReason = "TRUNCATE TABLE causes an implicit commit in MySQL"
	return true
}

func (a *StatementAnalyzer) analyzeDMLNode(node ast.StmtNode, analysis *StatementAnalysis) bool {
	switch node.(type) {
	case *ast.DeleteStmt:
		analysis.StatementType = "DELETE"
		analysis.IsDestructive = true
		analysis.DestructiveReason = "DELETE will remove rows from the table"
		return true
	case *ast.InsertStmt:
		analysis.StatementType = "INSERT"
		analysis.IsTransactionSafe = true
		return true
	case *ast.UpdateStmt:
		analysis.StatementType = "UPDATE"
		analysis.IsTransactionSafe = true
		return true
	case *ast.SelectStmt:
		analysis.StatementType = "SELECT"
		analysis.IsTransactionSafe = true
		return true
	default:
		return false
	}
}

func (a *StatementAnalyzer) analyzeOtherStatement(analysis *StatementAnalysis, originalSQL string) {
	analysis.StatementType = "OTHER"
	upper := strings.ToUpper(strings.TrimSpace(originalSQL))

	for keyword, txReason := range ddlImplicitCommitReasons {
		if strings.HasPrefix(upper, keyword) {
			analysis.StatementType = keyword
			analysis.IsTransactionSafe = false
			analysis.TxUnsafeReason = txReason
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

func (a *StatementAnalyzer) analyzeAlterTable(stmt *ast.AlterTableStmt, analysis *StatementAnalysis) {
	for _, spec := range stmt.Specs {
		a.analyzeAlterTableSpec(spec, analysis)
	}
}

func (a *StatementAnalyzer) analyzeAlterTableSpec(spec *ast.AlterTableSpec, analysis *StatementAnalysis) {
	if spec.Tp == ast.AlterTableAddConstraint {
		a.analyzeAddConstraint(spec, analysis)
		return
	}

	effect, ok := alterTableSpecEffects[spec.Tp]
	if !ok {
		return
	}

	if effect.blocking {
		analysis.IsBlocking = true
	}
	if effect.destructive {
		analysis.IsDestructive = true
		analysis.DestructiveReason = effect.destructiveReason
	}
	if effect.blockingReason != "" {
		analysis.BlockingReasons = append(analysis.BlockingReasons, effect.blockingReason)
	}
}

func (a *StatementAnalyzer) analyzeAddConstraint(spec *ast.AlterTableSpec, analysis *StatementAnalysis) {
	analysis.IsBlocking = true
	if spec.Constraint == nil {
		analysis.BlockingReasons = append(analysis.BlockingReasons,
			"ADD CONSTRAINT may lock the table while validating existing data")
		return
	}

	switch spec.Constraint.Tp {
	case ast.ConstraintForeignKey:
		analysis.BlockingReasons = append(analysis.BlockingReasons,
			"ADD FOREIGN KEY may lock the table while validating existing data")
	case ast.ConstraintIndex, ast.ConstraintKey, ast.ConstraintUniq,
		ast.ConstraintUniqKey, ast.ConstraintUniqIndex:
		analysis.BlockingReasons = append(analysis.BlockingReasons,
			"ADD INDEX may lock the table for the duration of index creation on large tables")
	default:
		analysis.BlockingReasons = append(analysis.BlockingReasons,
			"ADD CONSTRAINT may lock the table while validating existing data")
	}
}
