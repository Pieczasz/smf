// Package apply adds a functionality to connect to a user database and perform
// an actual migration on the database. User can decide upon different settings,
// so the migration can be as safe as possible and reversible.
package apply

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"strings"

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
}

// NewApplier returns a pointer to Applier for user use, with provided options.
func NewApplier(options Options) *Applier {
	out := options.Out
	if out == nil {
		out = io.Discard
	}
	return &Applier{
		options:  options,
		analyzer: NewStatementAnalyzer(),
		out:      out,
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
	if a.options.DryRun {
		return a.dryRun(statements, preflight)
	}

	if a.options.Transaction && !preflight.IsTransactional {
		if !a.options.AllowNonTransactional {
			return fmt.Errorf("migration contains non-transactional DDL statements; use --allow-non-transactional to proceed")
		}
	}

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
			return fmt.Errorf("failed to ping database: %v; additionally failed to close connection: %w", pingErr, closeErr)
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

	return a.parseHumanMigration(content)
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

func (a *Applier) parseHumanMigration(content string) []string {
	statements := a.splitStatementsWithParser(content)
	a.statements = statements
	return statements
}

func (a *Applier) splitStatementsWithParser(content string) []string {
	var statements []string
	content = strings.TrimSpace(content)

	// TODO: add support for charset and collation
	stmtNodes, _, err := a.analyzer.parser.Parse(content, "", "")
	if err == nil && len(stmtNodes) > 0 {
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
		if len(statements) > 0 {
			return statements
		}
	}

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

func truncateSQL(stmt string) string {
	stmt = strings.TrimSpace(stmt)
	if len(stmt) > 80 {
		return stmt[:77] + "..."
	}
	return stmt
}

func (a *Applier) dryRun(statements []string, preflight *PreflightResult) error {
	a.println("=== DRY RUN MODE ===")

	a.println("--- Preflight Checks ---")
	if len(preflight.Warnings) == 0 {
		a.println("No warnings")
	} else {
		for _, w := range preflight.Warnings {
			a.printf("[%s] %s\n", w.Level, w.Message)
			if w.SQL != "" {
				a.printf("    SQL: %s\n", w.SQL)
			}
		}
	}

	a.println("--- Transaction Safety ---")
	if preflight.IsTransactional {
		a.println("All statements are transaction-safe")
	} else {
		a.println("Migration is NOT transaction-safe")
		for _, reason := range preflight.NonTxReasons {
			a.printf("  - %s\n", reason)
		}
	}

	a.println("--- Statements to Execute ---")
	for i, stmt := range statements {
		a.printf("%d. %s\n\n", i+1, stmt)
	}

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

	a.println("=== DRY RUN COMPLETE ===")
	a.println("All preflight checks passed. Run without --dry-run to apply.")
	return nil
}

func (a *Applier) applyWithTransaction(ctx context.Context, statements []string) error {
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	for i, stmt := range statements {
		a.printf("Executing statement %d/%d...\n", i+1, len(statements))
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			if rbErr := tx.Rollback(); rbErr != nil {
				return fmt.Errorf("execute failed: %w; rollback also failed: %v", err, rbErr)
			}
			return fmt.Errorf("execute failed (rolled back): %w\n  Statement: %s", err, truncateSQL(stmt))
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	a.printf("Successfully applied %d statements\n", len(statements))
	return nil
}

func (a *Applier) applyWithoutTransaction(ctx context.Context, statements []string) error {
	a.println("Applying migration without transaction wrapper (DDL statements cause implicit commits)")

	successCount := 0
	for i, stmt := range statements {
		a.printf("Executing statement %d/%d...\n", i+1, len(statements))
		if _, err := a.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("statement %d failed: %w\n  Statement: %s\n  %d statements were already applied and cannot be automatically rolled back",
				i+1, err, truncateSQL(stmt), successCount)
		}
		successCount++
	}

	a.printf("Successfully applied %d statements\n", len(statements))
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
