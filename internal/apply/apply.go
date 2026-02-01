// Package apply adds a functionality to connect to a user database and perform
// an actual migration on the database. User can decide upon different settings,
// so the migration can be as safe as possible and reversible.
package apply

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/pingcap/tidb/pkg/parser/format"
)

// PreflightResult contains a list of warnings, errors, and transactionality info about migration.
type PreflightResult struct {
	Warnings        []Warning
	Errors          []string
	IsTransactional bool
	NonTxReasons    []string
}

// Warning contains a Level of a warning, message, and actual SQL from migration.
type Warning struct {
	Level   WarningLevel
	Message string
	SQL     string
}

// WarningLevel is a const that is expandable for later and contains different levels of danger.
type WarningLevel string

const (
	WarnCaution WarningLevel = "CAUTION"
	WarnDanger  WarningLevel = "DANGER"
)

// Options struct contains all setting available for user to choose during apply command.
type Options struct {
	DSN                   string
	FilePath              string
	DryRun                bool
	Transaction           bool
	AllowNonTransactional bool
	Unsafe                bool
	Out                   io.Writer
	In                    io.Reader
	SkipConfirmation      bool
}

type jsonMigration struct {
	Format  string   `json:"format"`
	SQL     []string `json:"sql,omitempty"`
	Summary struct {
		SQLStatements int `json:"sqlStatements"`
	} `json:"summary"`
}

// Applier is a struct that contains data from a user to apply actual migration.
type Applier struct {
	db         *sql.DB
	statements []string
	options    Options
	analyzer   *StatementAnalyzer
	out        io.Writer
	in         io.Reader
}

// NewApplier returns a pointer to Applier for user use, with provided options.
func NewApplier(options Options) *Applier {
	out := options.Out
	if out == nil {
		out = io.Discard
	}
	in := options.In
	if in == nil {
		in = os.Stdin
	}
	return &Applier{
		options:  options,
		analyzer: NewStatementAnalyzer(),
		out:      out,
		in:       in,
	}
}

// We use custom printf to format and print messages to the output writer.
func (a *Applier) printf(format string, args ...any) {
	_, _ = fmt.Fprintf(a.out, format, args...)
}

func (a *Applier) println(args ...any) {
	_, _ = fmt.Fprintln(a.out, args...)
}

// Apply function, look for the dryRun option, runs it, and
// depending on a transactional option, run the appropriate migration.
// If something went wrong, returns an error, otherwise nil.
func (a *Applier) Apply(ctx context.Context, statements []string, preflight *PreflightResult) error {
	a.displayPreflightChecks(preflight)
	a.displayStatements(statements)

	if a.options.DryRun {
		a.println("\n=== DRY RUN MODE ===")
		a.println("Run without --dry-run to apply.")
		return a.validatePreflight(preflight)
	}

	if a.options.Transaction && !preflight.IsTransactional {
		if !a.options.AllowNonTransactional {
			return fmt.Errorf("migration contains non-transactional DDL statements; use --allow-non-transactional to proceed")
		}
	}

	// Validate preflight before asking for confirmation
	if err := a.validatePreflight(preflight); err != nil {
		return err
	}

	// Ask for confirmation
	if !a.options.SkipConfirmation {
		if !a.askConfirmation() {
			a.println("\nMigration canceled.")
			return nil
		}
	}

	a.println("\nExecuting...")

	if a.options.Transaction && preflight.IsTransactional {
		return a.applyWithTransaction(ctx, statements)
	}

	return a.applyWithoutTransaction(ctx, statements)
}

// Connect establishes a connection with a user database and pings it to test a connection.
// If something went wrong, returns an error, otherwise nil.
func (a *Applier) Connect(ctx context.Context) error {
	db, err := sql.Open("mysql", a.options.DSN)
	if err != nil {
		return fmt.Errorf("failed to open database connection: %w", err)
	}

	if pingErr := db.PingContext(ctx); pingErr != nil {
		if closeErr := db.Close(); closeErr != nil {
			return fmt.Errorf("failed to ping database: %w; additionally failed to close connection: %w", pingErr, closeErr)
		}
		return fmt.Errorf("failed to ping database: %w", pingErr)
	}

	a.db = db
	return nil
}

// Close closes a connection with a database from applier
// If something went wrong, returns an error, otherwise nil.
func (a *Applier) Close() error {
	if a.db != nil {
		return a.db.Close()
	}
	return nil
}

func (a *Applier) ParseStatements(content string) []string {
	content = strings.TrimSpace(content)

	var migration jsonMigration
	if err := json.Unmarshal([]byte(content), &migration); err == nil {
		if migration.Format == "json" {
			statements := a.extractJSONStatements(&migration)
			if len(statements) > 0 {
				a.statements = statements
				return statements
			}
		}
	}

	return a.parseSQLMigration(content)
}

// PreflightChecks uses the AST-based analyzer to detect dangerous operations
// and transaction safety issues in the provided SQL statements.
func (a *Applier) PreflightChecks(statements []string, unsafe bool) *PreflightResult {
	return a.analyzer.AnalyzeStatements(statements, unsafe)
}

func (a *Applier) extractJSONStatements(migration *jsonMigration) []string {
	var statements []string
	for _, stmt := range migration.SQL {
		stmt = strings.TrimSpace(stmt)
		if stmt != "" {
			statements = append(statements, stmt)
		}
	}
	return statements
}

func (a *Applier) parseSQLMigration(content string) []string {
	statements := a.splitStatementsWithParser(content)
	a.statements = statements
	return statements
}

func (a *Applier) splitStatementsWithParser(content string) []string {
	content = strings.TrimSpace(content)
	if statements := a.splitStatementsUsingTiDBParser(content); len(statements) > 0 {
		return statements
	}
	return splitStatementsBySemicolon(content)
}

func (a *Applier) splitStatementsUsingTiDBParser(content string) []string {
	// TODO: add support for charset and collation
	stmtNodes, _, err := a.analyzer.parser.Parse(content, "", "")
	if err != nil || len(stmtNodes) == 0 {
		return nil
	}

	statements := make([]string, 0, len(stmtNodes))
	for _, node := range stmtNodes {
		if node == nil {
			continue
		}
		var sb strings.Builder
		ctx := format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)
		if restoreErr := node.Restore(ctx); restoreErr != nil {
			continue
		}
		stmt := strings.TrimSpace(sb.String())
		if stmt != "" {
			statements = append(statements, stmt)
		}
	}

	if len(statements) == 0 {
		return nil
	}
	return statements
}

func splitStatementsBySemicolon(content string) []string {
	var statements []string
	var current strings.Builder

	for line := range strings.SplitSeq(content, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "--") || trimmed == "" {
			continue
		}

		current.WriteString(line)
		current.WriteString("\n")
		if strings.HasSuffix(trimmed, ";") {
			stmt := strings.TrimSpace(current.String())
			if stmt != "" {
				statements = append(statements, stmt)
			}
			current.Reset()
		}
	}

	if remaining := strings.TrimSpace(current.String()); remaining != "" {
		statements = append(statements, remaining)
	}
	return statements
}

func truncateSQL(stmt string, maxLen int) string {
	stmt = strings.TrimSpace(stmt)
	if maxLen <= 0 {
		maxLen = 60
	}
	if len(stmt) > maxLen {
		return stmt[:maxLen-3] + "..."
	}
	return stmt
}

func (a *Applier) displayPreflightChecks(preflight *PreflightResult) {
	a.println("Preflight checks:")

	if a.db != nil {
		a.println("  OK: Database is accessible")
	}

	if len(a.statements) > 0 || len(preflight.Errors) == 0 {
		a.println("  OK: All migrations are valid SQL")
	}

	for _, err := range preflight.Errors {
		a.printf("  ERROR: %s\n", err)
	}

	for _, w := range preflight.Warnings {
		if w.Level == WarnDanger {
			a.printf("  DANGER: %s\n", w.Message)
		} else {
			a.printf("  WARNING: %s\n", w.Message)
		}
	}

	if !preflight.IsTransactional {
		a.println("  WARNING: Migration is NOT transaction-safe")
		for _, reason := range preflight.NonTxReasons {
			a.printf("    - %s\n", reason)
		}
	}
}

func (a *Applier) displayStatements(statements []string) {
	a.println("\nStatements to execute:")
	for i, stmt := range statements {
		a.printf("  %d. %s\n", i+1, stmt)
	}
}

func (a *Applier) validatePreflight(preflight *PreflightResult) error {
	hasDestructive := false
	for _, w := range preflight.Warnings {
		if w.Level == WarnDanger && !a.options.Unsafe {
			hasDestructive = true
			break
		}
	}

	if hasDestructive {
		return fmt.Errorf("preflight checks failed: destructive operations detected without --unsafe flag")
	}

	if a.options.Transaction && !preflight.IsTransactional && !a.options.AllowNonTransactional {
		return fmt.Errorf("preflight checks failed: non-transactional DDL detected without --allow-non-transactional flag")
	}

	return nil
}

func (a *Applier) askConfirmation() bool {
	a.printf("\nExecute? [y/n]: ")
	reader := bufio.NewReader(a.in)
	response, err := reader.ReadString('\n')
	if err != nil {
		return false
	}
	response = strings.TrimSpace(strings.ToLower(response))
	return response == "y" || response == "yes"
}

func (a *Applier) applyWithTransaction(ctx context.Context, statements []string) error {
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	total := len(statements)
	for i, stmt := range statements {
		start := time.Now()
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			a.printf("  [%d/%d] FAILED: %s\n", i+1, total, truncateSQL(stmt, 50))
			if rbErr := tx.Rollback(); rbErr != nil {
				return fmt.Errorf("execute failed: %w; rollback also failed: %w", err, rbErr)
			}
			return fmt.Errorf("execute failed (rolled back): %w\n  Statement: %s", err, truncateSQL(stmt, 80))
		}
		elapsed := time.Since(start)
		a.printf("  [%d/%d] OK: %s (%.2fs)\n", i+1, total, truncateSQL(stmt, 50), elapsed.Seconds())
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	a.println("\nMigration complete!")
	return nil
}

func (a *Applier) applyWithoutTransaction(ctx context.Context, statements []string) error {
	total := len(statements)
	successCount := 0
	for i, stmt := range statements {
		start := time.Now()
		if _, err := a.db.ExecContext(ctx, stmt); err != nil {
			a.printf("  [%d/%d] FAILED: %s\n", i+1, total, truncateSQL(stmt, 50))
			return fmt.Errorf("statement %d failed: %w\n  Statement: %s\n  %d statements were already applied and cannot be automatically rolled back",
				i+1, err, truncateSQL(stmt, 80), successCount)
		}
		elapsed := time.Since(start)
		a.printf("  [%d/%d] OK: %s (%.2fs)\n", i+1, total, truncateSQL(stmt, 50), elapsed.Seconds())
		successCount++
	}

	a.println("\nMigration complete!")
	return nil
}

// HasDestructiveOperations checks if there is a dangerous warning inside a preflight
// analysis of a migration. If it has returns true, otherwise false.
func HasDestructiveOperations(preflight *PreflightResult) bool {
	for _, w := range preflight.Warnings {
		if w.Level == WarnDanger {
			return true
		}
	}
	return false
}
