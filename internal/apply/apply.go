// Package apply adds a functionality to connect to a user database and perform
// an actual migration on the database. User can decide upon different settings,
// so the migration can be as safe as possible and reversible.
package apply

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
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
}

// NewApplier returns a pointer to Applier for user use, with provided options.
func NewApplier(options Options) *Applier {
	return &Applier{
		options: options,
	}
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

	if err := db.PingContext(ctx); err != nil {
		err := db.Close()
		if err != nil {
			return err
		}
		return fmt.Errorf("failed to ping database: %w", err)
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

func (a *Applier) PreflightChecks(statements []string, unsafe bool) *PreflightResult {
	result := &PreflightResult{
		IsTransactional: true,
	}

	for _, stmt := range statements {
		upper := strings.ToUpper(stmt)

		blockingWarnings := checkBlockingDDL(stmt, upper)
		result.Warnings = append(result.Warnings, blockingWarnings...)

		destructiveWarnings := checkDestructive(stmt, upper, unsafe)
		result.Warnings = append(result.Warnings, destructiveWarnings...)

		if !isTransactionSafe(upper) {
			result.IsTransactional = false
			reason := getTransactionUnsafeReason(upper, stmt)
			result.NonTxReasons = append(result.NonTxReasons, reason)
		}
	}

	return result
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
	var statements []string
	var current strings.Builder

	lines := strings.Split(content, "\n")
	for _, line := range lines {
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

	a.statements = statements
	return statements
}

func checkBlockingDDL(stmt, upper string) []Warning {
	var warnings []Warning

	blockingPatterns := []struct {
		pattern string
		message string
	}{
		{
			pattern: `ALTER\s+TABLE\s+\S+\s+ADD\s+(INDEX|KEY)`,
			message: "ADD INDEX may lock the table for the duration of index creation on large tables",
		},
		{
			pattern: `ALTER\s+TABLE\s+\S+\s+DROP\s+(INDEX|KEY)`,
			message: "DROP INDEX may briefly lock the table",
		},
		{
			pattern: `ALTER\s+TABLE\s+\S+\s+ADD\s+COLUMN`,
			message: "ADD COLUMN may require a table rebuild depending on MySQL version and column position",
		},
		{
			pattern: `ALTER\s+TABLE\s+\S+\s+DROP\s+COLUMN`,
			message: "DROP COLUMN typically requires a full table rebuild and will lock the table",
		},
		{
			pattern: `ALTER\s+TABLE\s+\S+\s+MODIFY`,
			message: "MODIFY COLUMN may require a table rebuild if changing column type or size",
		},
		{
			pattern: `ALTER\s+TABLE\s+\S+\s+CHANGE`,
			message: "CHANGE COLUMN may require a table rebuild",
		},
		{
			pattern: `ALTER\s+TABLE\s+\S+\s+ADD\s+(CONSTRAINT|FOREIGN\s+KEY)`,
			message: "ADD CONSTRAINT/FOREIGN KEY may lock the table while validating existing data",
		},
		{
			pattern: `CREATE\s+INDEX`,
			message: "CREATE INDEX may lock the table for the duration of index creation",
		},
		{
			pattern: `DROP\s+INDEX`,
			message: "DROP INDEX may briefly lock the table",
		},
		{
			pattern: `RENAME\s+TABLE`,
			message: "RENAME TABLE acquires an exclusive lock but is typically fast",
		},
		{
			pattern: `TRUNCATE\s+TABLE`,
			message: "TRUNCATE TABLE acquires an exclusive lock and removes all data instantly",
		},
	}

	for _, bp := range blockingPatterns {
		matched, _ := regexp.MatchString(bp.pattern, upper)
		if matched {
			warnings = append(warnings, Warning{
				Level:   WarnCaution,
				Message: fmt.Sprintf("Potentially blocking DDL: %s", bp.message),
				SQL:     truncateSQL(stmt),
			})
		}
	}

	return warnings
}

func checkDestructive(stmt, upper string, unsafeAllowed bool) []Warning {
	var warnings []Warning

	destructivePatterns := []struct {
		pattern string
		message string
	}{
		{
			pattern: `DROP\s+TABLE`,
			message: "DROP TABLE will permanently delete the table and all its data",
		},
		{
			pattern: `DROP\s+DATABASE`,
			message: "DROP DATABASE will permanently delete the entire database",
		},
		{
			pattern: `DROP\s+COLUMN`,
			message: "DROP COLUMN will permanently delete the column and its data",
		},
		{
			pattern: `TRUNCATE\s+TABLE`,
			message: "TRUNCATE TABLE will delete all rows from the table",
		},
		{
			pattern: `DELETE\s+FROM`,
			message: "DELETE will remove rows from the table",
		},
	}

	for _, dp := range destructivePatterns {
		matched, _ := regexp.MatchString(dp.pattern, upper)
		if matched {
			level := WarnDanger
			msg := dp.message
			if !unsafeAllowed {
				msg = fmt.Sprintf("%s (requires --unsafe flag)", dp.message)
			}
			warnings = append(warnings, Warning{
				Level:   level,
				Message: msg,
				SQL:     truncateSQL(stmt),
			})
		}
	}

	return warnings
}

func isTransactionSafe(upper string) bool {
	nonTransactionalPatterns := []string{
		`CREATE\s+DATABASE`,
		`DROP\s+DATABASE`,
		`ALTER\s+DATABASE`,
		`CREATE\s+TABLE`,
		`DROP\s+TABLE`,
		`ALTER\s+TABLE`,
		`RENAME\s+TABLE`,
		`TRUNCATE\s+TABLE`,
		`CREATE\s+INDEX`,
		`DROP\s+INDEX`,
		`CREATE\s+VIEW`,
		`DROP\s+VIEW`,
		`ALTER\s+VIEW`,
		`CREATE\s+PROCEDURE`,
		`DROP\s+PROCEDURE`,
		`ALTER\s+PROCEDURE`,
		`CREATE\s+FUNCTION`,
		`DROP\s+FUNCTION`,
		`ALTER\s+FUNCTION`,
		`CREATE\s+TRIGGER`,
		`DROP\s+TRIGGER`,
		`CREATE\s+EVENT`,
		`DROP\s+EVENT`,
		`ALTER\s+EVENT`,
	}

	for _, pattern := range nonTransactionalPatterns {
		matched, _ := regexp.MatchString(pattern, upper)
		if matched {
			return false
		}
	}

	return true
}

func getTransactionUnsafeReason(upper, stmt string) string {
	reasonMap := map[string]string{
		`CREATE\s+TABLE`:     "CREATE TABLE causes an implicit commit in MySQL",
		`DROP\s+TABLE`:       "DROP TABLE causes an implicit commit in MySQL",
		`ALTER\s+TABLE`:      "ALTER TABLE causes an implicit commit in MySQL",
		`RENAME\s+TABLE`:     "RENAME TABLE causes an implicit commit in MySQL",
		`TRUNCATE\s+TABLE`:   "TRUNCATE TABLE causes an implicit commit in MySQL",
		`CREATE\s+INDEX`:     "CREATE INDEX causes an implicit commit in MySQL",
		`DROP\s+INDEX`:       "DROP INDEX causes an implicit commit in MySQL",
		`CREATE\s+DATABASE`:  "CREATE DATABASE causes an implicit commit in MySQL",
		`DROP\s+DATABASE`:    "DROP DATABASE causes an implicit commit in MySQL",
		`ALTER\s+DATABASE`:   "ALTER DATABASE causes an implicit commit in MySQL",
		`CREATE\s+VIEW`:      "CREATE VIEW causes an implicit commit in MySQL",
		`DROP\s+VIEW`:        "DROP VIEW causes an implicit commit in MySQL",
		`CREATE\s+PROCEDURE`: "CREATE PROCEDURE causes an implicit commit in MySQL",
		`DROP\s+PROCEDURE`:   "DROP PROCEDURE causes an implicit commit in MySQL",
		`CREATE\s+FUNCTION`:  "CREATE FUNCTION causes an implicit commit in MySQL",
		`DROP\s+FUNCTION`:    "DROP FUNCTION causes an implicit commit in MySQL",
		`CREATE\s+TRIGGER`:   "CREATE TRIGGER causes an implicit commit in MySQL",
		`DROP\s+TRIGGER`:     "DROP TRIGGER causes an implicit commit in MySQL",
	}

	for pattern, reason := range reasonMap {
		matched, _ := regexp.MatchString(pattern, upper)
		if matched {
			return fmt.Sprintf("%s: %s", reason, truncateSQL(stmt))
		}
	}

	return fmt.Sprintf("DDL statement causes implicit commit: %s", truncateSQL(stmt))
}

func truncateSQL(stmt string) string {
	stmt = strings.TrimSpace(stmt)
	if len(stmt) > 80 {
		return stmt[:77] + "..."
	}
	return stmt
}

func (a *Applier) dryRun(statements []string, preflight *PreflightResult) error {
	fmt.Println("=== DRY RUN MODE ===")
	fmt.Println()

	fmt.Println("--- Preflight Checks ---")
	if len(preflight.Warnings) == 0 {
		fmt.Println("No warnings")
	} else {
		for _, w := range preflight.Warnings {
			fmt.Printf("[%s] %s\n", w.Level, w.Message)
			if w.SQL != "" {
				fmt.Printf("    SQL: %s\n", w.SQL)
			}
		}
	}
	fmt.Println()

	fmt.Println("--- Transaction Safety ---")
	if preflight.IsTransactional {
		fmt.Println("All statements are transaction-safe")
	} else {
		fmt.Println("Migration is NOT transaction-safe")
		for _, reason := range preflight.NonTxReasons {
			fmt.Printf("  - %s\n", reason)
		}
	}
	fmt.Println()

	fmt.Println("--- Statements to Execute ---")
	for i, stmt := range statements {
		fmt.Printf("%d. %s\n\n", i+1, stmt)
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

	fmt.Println("=== DRY RUN COMPLETE ===")
	fmt.Println("All preflight checks passed. Run without --dry-run to apply.")
	return nil
}

func (a *Applier) applyWithTransaction(ctx context.Context, statements []string) error {
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	for i, stmt := range statements {
		fmt.Printf("Executing statement %d/%d...\n", i+1, len(statements))
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

	fmt.Printf("Successfully applied %d statements\n", len(statements))
	return nil
}

func (a *Applier) applyWithoutTransaction(ctx context.Context, statements []string) error {
	fmt.Println("Applying migration without transaction wrapper (DDL statements cause implicit commits)")
	fmt.Println()

	successCount := 0
	for i, stmt := range statements {
		fmt.Printf("Executing statement %d/%d...\n", i+1, len(statements))
		if _, err := a.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("statement %d failed: %w\n  Statement: %s\n  %d statements were already applied and cannot be automatically rolled back",
				i+1, err, truncateSQL(stmt), successCount)
		}
		successCount++
	}

	fmt.Printf("Successfully applied %d statements\n", len(statements))
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
