package main

import (
	"context"
	"fmt"
	"os"
	"smf/apply"
	"smf/dialect"
	"smf/dialect/mysql"
	"smf/diff"
	"smf/output"
	"smf/parser"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"
)

func printInfo(format string, msg string) {
	if strings.EqualFold(strings.TrimSpace(format), string(output.FormatJSON)) {
		_, _ = fmt.Fprintln(os.Stderr, msg)
		return
	}
	fmt.Println(msg)
}

func main() {
	rootCmd := &cobra.Command{
		Use:   "smf",
		Short: "Database migration tool",
	}

	var diffOutFile string
	var diffFormat string
	diffCmd := &cobra.Command{
		Use:   "diff <old.sql> <new.sql>",
		Short: "Compare two schemas",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			oldData, err := os.ReadFile(args[0])
			if err != nil {
				return fmt.Errorf("failed to read old schema: %w", err)
			}
			newData, err := os.ReadFile(args[1])
			if err != nil {
				return fmt.Errorf("failed to read new schema: %w", err)
			}

			p := parser.NewSQLParser()
			oldDB, err := p.ParseSchema(string(oldData))
			if err != nil {
				return fmt.Errorf("parse old schema error: %w", err)
			}
			newDB, err := p.ParseSchema(string(newData))
			if err != nil {
				return fmt.Errorf("parse new schema error: %w", err)
			}

			schemaDiff := diff.Diff(oldDB, newDB)
			formatter, err := output.NewFormatter(diffFormat)
			if err != nil {
				return err
			}
			formatted, err := formatter.FormatDiff(schemaDiff)
			if err != nil {
				return fmt.Errorf("failed to format output: %w", err)
			}
			if diffOutFile == "" {
				fmt.Print(formatted)
				return nil
			}
			if err := os.WriteFile(diffOutFile, []byte(formatted), 0644); err != nil {
				return fmt.Errorf("failed to write output: %w", err)
			}
			printInfo(diffFormat, fmt.Sprintf("Output saved to %s", diffOutFile))
			return nil
		},
	}

	diffCmd.Flags().StringVarP(&diffOutFile, "output", "o", "", "Output file for the diff")
	diffCmd.Flags().StringVarP(&diffFormat, "format", "f", "", "Output format: json or human")

	var fromDialect string
	var toDialect string
	var migrationOutFile string
	var rollbackOutFile string
	var migrationFormat string
	var migrateUnsafe bool

	migrateCmd := &cobra.Command{
		Use:   "migrate <old.sql> <new.sql>",
		Short: "Migrate schema from old dump to new dump",
		Long: `Migrate generates the necessary SQL statements to transition a database schema 
from an old state (old.sql) to a new state (new.sql).
You can specify the source and target database dialects using the --from and --to flags.`,
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			oldPath := args[0]
			newPath := args[1]

			printInfo(migrationFormat, fmt.Sprintf("Migrating from %s (%s) to %s (%s)", oldPath, fromDialect, newPath, toDialect))

			supported := map[string]bool{"mysql": true}
			if !supported[strings.ToLower(fromDialect)] {
				return fmt.Errorf("unsupported source dialect: %s", fromDialect)
			}
			if !supported[strings.ToLower(toDialect)] {
				return fmt.Errorf("unsupported target dialect: %s", toDialect)
			}

			oldData, err := os.ReadFile(oldPath)
			if err != nil {
				return fmt.Errorf("failed to read old schema: %w", err)
			}
			newData, err := os.ReadFile(newPath)
			if err != nil {
				return fmt.Errorf("failed to read new schema: %w", err)
			}

			// TODO: Use appropriate parser based on fromDialect and toDialect
			p := parser.NewSQLParser()
			oldDB, err := p.ParseSchema(string(oldData))
			if err != nil {
				return fmt.Errorf("failed to parse old schema: %w", err)
			}
			newDB, err := p.ParseSchema(string(newData))
			if err != nil {
				return fmt.Errorf("failed to parse new schema: %w", err)
			}

			schemaDiff := diff.Diff(oldDB, newDB)
			printInfo(migrationFormat, fmt.Sprintf("Detected changes between schemas (old: %d tables, new: %d tables)",
				len(oldDB.Tables), len(newDB.Tables)))

			d := mysql.NewMySQLDialect()
			opts := dialect.DefaultMigrationOptions(dialect.MySQL)
			opts.IncludeUnsafe = migrateUnsafe
			migration := d.Generator().GenerateMigrationWithOptions(schemaDiff, opts)

			formatter, err := output.NewFormatter(migrationFormat)
			if err != nil {
				return err
			}
			formatted, err := formatter.FormatMigration(migration)
			if err != nil {
				return fmt.Errorf("failed to format output: %w", err)
			}
			if migrationOutFile == "" {
				fmt.Print(formatted)
				return nil
			}
			if err := os.WriteFile(migrationOutFile, []byte(formatted), 0644); err != nil {
				return fmt.Errorf("failed to write output: %w", err)
			}
			printInfo(migrationFormat, fmt.Sprintf("Output saved to %s", migrationOutFile))
			if rollbackOutFile != "" {
				if err := migration.SaveRollbackToFile(rollbackOutFile); err != nil {
					return fmt.Errorf("failed to write rollback output: %w", err)
				}
				printInfo(migrationFormat, fmt.Sprintf("Rollback saved to %s", rollbackOutFile))
			}
			return nil
		},
	}

	migrateCmd.Flags().StringVar(&fromDialect, "from", "mysql", "Source database dialect (e.g., mysql)")
	migrateCmd.Flags().StringVarP(&toDialect, "to", "t", "mysql", "Target database dialect (e.g., mysql)")
	migrateCmd.Flags().StringVarP(&migrationOutFile, "output", "o", "", "Output file for the generated migration SQL")
	migrateCmd.Flags().StringVarP(&rollbackOutFile, "rollback-output", "r", "", "Output file for generated rollback SQL (run separately)")
	migrateCmd.Flags().StringVarP(&migrationFormat, "format", "f", "", "Output format: json or human")
	migrateCmd.Flags().BoolVarP(&migrateUnsafe, "unsafe", "u", false, "Generate unsafe migration (may drop/overwrite data); safe mode by default")

	//_ = migrateCmd.MarkFlagRequired("from")
	//_ = migrateCmd.MarkFlagRequired("to")

	var applyDSN string
	var applyFile string
	var applyDryRun bool
	var applyTransaction bool
	var applyAllowNonTransactional bool
	var applyUnsafe bool
	var applyTimeout int

	applyCmd := &cobra.Command{
		Use:   "apply",
		Short: "Apply a database schema migration",
		Long: `Connects to your database and applies a migration file.

This command performs preflight checks before execution:
- Warns about potentially blocking DDL operations
- Warns about destructive operations (DROP, TRUNCATE, etc.)
- Checks transaction safety of the migration

Examples:
  smf apply --dsn "user:pass@tcp(localhost:3306)/mydb" --file migration.sql
  smf apply --dsn "user:pass@tcp(localhost:3306)/mydb" --file migration.sql --dry-run
  smf apply --dsn "user:pass@tcp(localhost:3306)/mydb" --file migration.sql --unsafe`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if applyDSN == "" {
				return fmt.Errorf("--dsn is required")
			}
			if applyFile == "" {
				return fmt.Errorf("--file is required")
			}

			content, err := os.ReadFile(applyFile)
			if err != nil {
				return fmt.Errorf("failed to read migration file: %w", err)
			}

			opts := apply.Options{
				DSN:                   applyDSN,
				FilePath:              applyFile,
				DryRun:                applyDryRun,
				Transaction:           applyTransaction,
				AllowNonTransactional: applyAllowNonTransactional,
				Unsafe:                applyUnsafe,
			}
			applier := apply.NewApplier(opts)

			statements := applier.ParseStatements(string(content))
			if len(statements) == 0 {
				fmt.Println("No SQL statements found in migration file")
				return nil
			}

			fmt.Printf("Found %d statement(s) in %s\n", len(statements), applyFile)
			fmt.Println()

			preflight := applier.PreflightChecks(statements, applyUnsafe)

			if apply.HasDestructiveOperations(preflight) && !applyUnsafe {
				fmt.Println("--- Preflight Warnings ---")
				for _, w := range preflight.Warnings {
					if w.Level == apply.WarnDanger {
						fmt.Printf("✗ [%s] %s\n", w.Level, w.Message)
						if w.SQL != "" {
							fmt.Printf("    SQL: %s\n", w.SQL)
						}
					}
				}
				return fmt.Errorf("destructive operations detected; use --unsafe to allow these operations")
			}

			if applyTransaction && !preflight.IsTransactional && !applyAllowNonTransactional {
				fmt.Println("--- Transaction Safety ---")
				fmt.Println("Migration is NOT transaction-safe:")
				for _, reason := range preflight.NonTxReasons {
					fmt.Printf("  - %s\n", reason)
				}
				fmt.Println()
				fmt.Println("MySQL DDL statements (CREATE, ALTER, DROP, etc.) cause implicit commits")
				fmt.Println("and cannot be rolled back within a transaction.")
				fmt.Println()
				fmt.Println("Options:")
				fmt.Println("  1. Use --allow-non-transactional to proceed without transaction protection")
				fmt.Println("  2. Use --transaction=false to explicitly disable transaction mode")
				return fmt.Errorf("non-transactional DDL detected; use --allow-non-transactional to proceed")
			}

			if applyDryRun {
				return applier.Apply(context.Background(), statements, preflight)
			}

			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(applyTimeout)*time.Second)
			defer cancel()

			fmt.Printf("Connecting to database...\n")
			if err := applier.Connect(ctx); err != nil {
				return err
			}
			defer func(applier *apply.Applier) {
				err := applier.Close()
				if err != nil {
					fmt.Printf("Failed to close database connection: %v\n", err)
				}
			}(applier)

			return applier.Apply(ctx, statements, preflight)
		},
	}

	applyCmd.Flags().StringVar(&applyDSN, "dsn", "", "Database connection string (required)")
	applyCmd.Flags().StringVarP(&applyFile, "file", "f", "", "Path to migration SQL file (required)")
	applyCmd.Flags().BoolVarP(&applyDryRun, "dry-run", "d", false, "Print statements and run preflight checks without executing")
	applyCmd.Flags().BoolVarP(&applyTransaction, "transaction", "t", true, "Run migration in a transaction if possible")
	applyCmd.Flags().BoolVar(&applyAllowNonTransactional, "allow-non-transactional", false, "Allow non-transactional DDL when --transaction is set")
	applyCmd.Flags().BoolVarP(&applyUnsafe, "unsafe", "u", false, "Allow destructive operations (DROP, TRUNCATE, etc.)")
	applyCmd.Flags().IntVar(&applyTimeout, "timeout", 300, "Connection timeout in seconds")

	rootCmd.AddCommand(diffCmd)
	rootCmd.AddCommand(migrateCmd)
	rootCmd.AddCommand(applyCmd)

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
