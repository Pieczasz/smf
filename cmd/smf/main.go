// Package main contains the cli implementation of the tool. It uses cobra
// package for cli tool implementation.
package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"

	"smf/internal/apply"
	"smf/internal/core"
	"smf/internal/dialect"
	_ "smf/internal/dialect/mysql"
	"smf/internal/diff"
	"smf/internal/migration"
	"smf/internal/output"
	"smf/internal/parser"
)

type diffFlags struct {
	outFile       string
	format        string
	detectRenames bool
	dialect       string
}

type migrateFlags struct {
	fromDialect   string
	toDialect     string
	outFile       string
	rollbackFile  string
	format        string
	unsafe        bool
	detectRenames bool
}

type applyFlags struct {
	dsn                   string
	file                  string
	dryRun                bool
	transaction           bool
	allowNonTransactional bool
	unsafe                bool
	timeout               int
}

func main() {
	rootCmd := &cobra.Command{
		Use:   "smf",
		Short: "Database migration tool",
	}

	rootCmd.AddCommand(diffCmd())
	rootCmd.AddCommand(migrateCmd())
	rootCmd.AddCommand(applyCmd())

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func diffCmd() *cobra.Command {
	flags := &diffFlags{}
	cmd := &cobra.Command{
		Use:   "diff <old.sql> <new.sql>",
		Short: "Compare two schemas",
		Args:  cobra.ExactArgs(2),
		RunE: func(_ *cobra.Command, args []string) error {
			return runDiff(args[0], args[1], flags)
		},
	}

	cmd.Flags().StringVarP(&flags.outFile, "output", "o", "", "Output file for the diff")
	cmd.Flags().StringVarP(&flags.format, "format", "f", "", "Output format: json or sql")
	cmd.Flags().BoolVarP(&flags.detectRenames, "detect-renames", "r", true, "Enable heuristic column rename detection")
	cmd.Flags().StringVar(&flags.dialect, "dialect", "mysql", "Database dialect (e.g., mysql)")

	return cmd
}

func runDiff(oldPath, newPath string, flags *diffFlags) error {
	if err := validateDialect(flags.dialect); err != nil {
		return fmt.Errorf("unsupported dialect: %s", flags.dialect)
	}

	oldFile, newFile, cleanup, err := openSchemaFiles(oldPath, newPath)
	if err != nil {
		return err
	}
	defer cleanup()

	oldDB, newDB, err := parseSchemas(oldFile, newFile)
	if err != nil {
		return err
	}

	schemaDiff := diff.Diff(oldDB, newDB, diff.Options{DetectColumnRenames: flags.detectRenames})
	formatter, err := output.NewFormatter(flags.format)
	if err != nil {
		return err
	}

	formattedDiff, err := formatter.FormatDiff(schemaDiff)
	if err != nil {
		return fmt.Errorf("failed to format output: %w", err)
	}

	return writeOutput(formattedDiff, flags.outFile, flags.format)
}

func migrateCmd() *cobra.Command {
	flags := &migrateFlags{}
	cmd := &cobra.Command{
		Use:   "migrate <old.sql> <new.sql>",
		Short: "Migrate schema from old dump to new dump",
		Long: `Migrate generates the necessary SQL statements to transition a database schema
from an old state (old.sql) to a new state (new.sql).
You can specify the source and target database dialects using the --from and --to flags.`,
		Args: cobra.ExactArgs(2),
		RunE: func(_ *cobra.Command, args []string) error {
			return runMigrate(args[0], args[1], flags)
		},
	}

	cmd.Flags().StringVar(&flags.fromDialect, "from", "mysql", "Source database dialect (e.g., mysql)")
	cmd.Flags().StringVarP(&flags.toDialect, "to", "t", "mysql", "Target database dialect (e.g., mysql)")
	cmd.Flags().StringVarP(&flags.outFile, "output", "o", "", "Output file for the generated migration SQL")
	cmd.Flags().StringVarP(&flags.rollbackFile, "rollback-output", "b", "", "Output file for generated rollback SQL (run separately)")
	cmd.Flags().StringVarP(&flags.format, "format", "f", "", "Output format: json or sql")
	cmd.Flags().BoolVarP(&flags.unsafe, "unsafe", "u", false, "Generate unsafe migration (may drop/overwrite data); safe mode by default")
	cmd.Flags().BoolVarP(&flags.detectRenames, "detect-renames", "r", true, "Enable heuristic column rename detection")

	return cmd
}

func openSchemaFiles(oldPath, newPath string) (oldFile, newFile *os.File, cleanup func(), err error) {
	oldFile, err = os.Open(oldPath)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to open old schema: %w", err)
	}
	newFile, err = os.Open(newPath)
	if err != nil {
		_ = oldFile.Close()
		return nil, nil, nil, fmt.Errorf("failed to open new schema: %w", err)
	}
	cleanup = func() {
		_ = oldFile.Close()
		_ = newFile.Close()
	}
	return oldFile, newFile, cleanup, nil
}

func runMigrate(oldPath, newPath string, flags *migrateFlags) error {
	printInfo(flags.format, fmt.Sprintf("migrating from %s (%s) to %s (%s)", oldPath, flags.fromDialect, newPath, flags.toDialect))

	if err := validateDialect(flags.fromDialect); err != nil {
		return fmt.Errorf("unsupported source dialect: %s", flags.fromDialect)
	}
	if err := validateDialect(flags.toDialect); err != nil {
		return fmt.Errorf("unsupported target dialect: %s", flags.toDialect)
	}

	oldFile, newFile, cleanup, err := openSchemaFiles(oldPath, newPath)
	if err != nil {
		return err
	}
	defer cleanup()

	oldDB, newDB, err := parseSchemas(oldFile, newFile)
	if err != nil {
		return err
	}

	schemaDiff := diff.Diff(oldDB, newDB, diff.Options{DetectColumnRenames: flags.detectRenames})
	printInfo(flags.format, fmt.Sprintf("detected changes between schemas (old: %d tables, new: %d tables)",
		len(oldDB.Tables), len(newDB.Tables)))

	generatedMigration, err := generateMigration(schemaDiff, flags.unsafe)
	if err != nil {
		return fmt.Errorf("failed to generate migration: %w", err)
	}

	if err := formatMigration(generatedMigration, flags.format, flags.outFile); err != nil {
		return fmt.Errorf("failed to format generatedMigration: %w", err)
	}

	if flags.rollbackFile != "" {
		rbFile, err := os.Create(flags.rollbackFile)
		if err != nil {
			return fmt.Errorf("failed to create rollback file: %w", err)
		}
		defer func(rbFile *os.File) {
			_ = rbFile.Close()
		}(rbFile)

		if err := output.WriteRollback(generatedMigration, rbFile); err != nil {
			return fmt.Errorf("failed to write rollback output: %w", err)
		}
		printInfo(flags.format, fmt.Sprintf("rollback saved to %s", flags.rollbackFile))
	}
	return nil
}

func applyCmd() *cobra.Command {
	flags := &applyFlags{}
	cmd := &cobra.Command{
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
		RunE: func(_ *cobra.Command, _ []string) error {
			return runApply(flags)
		},
	}
	cmd.Flags().StringVar(&flags.dsn, "dsn", "", "Database connection string (required)")
	cmd.Flags().StringVarP(&flags.file, "file", "f", "", "Path to migration SQL file (required)")
	cmd.Flags().BoolVarP(&flags.dryRun, "dry-run", "d", false, "Print statements and run preflight checks without executing")
	cmd.Flags().BoolVarP(&flags.transaction, "transaction", "t", true, "Run migration in a transaction if possible")
	cmd.Flags().BoolVar(&flags.allowNonTransactional, "allow-non-transactional", false, "Allow non-transactional DDL when --transaction is set")
	cmd.Flags().BoolVarP(&flags.unsafe, "unsafe", "u", false, "Allow destructive operations (DROP, TRUNCATE, etc.)")
	cmd.Flags().IntVar(&flags.timeout, "timeout", 300, "Connection timeout in seconds")
	return cmd
}

func runApply(flags *applyFlags) error {
	if flags.dsn == "" {
		return fmt.Errorf("--dsn is required")
	}
	if flags.file == "" {
		return fmt.Errorf("--file is required")
	}

	f, err := os.Open(flags.file)
	if err != nil {
		return fmt.Errorf("failed to open migration file: %w", err)
	}
	defer func(f *os.File) {
		_ = f.Close()
	}(f)

	content, err := io.ReadAll(f)
	if err != nil {
		return fmt.Errorf("failed to read migration file: %w", err)
	}

	applier := apply.NewApplier(apply.Options{
		DSN:                   flags.dsn,
		DryRun:                flags.dryRun,
		Transaction:           flags.transaction,
		AllowNonTransactional: flags.allowNonTransactional,
		Unsafe:                flags.unsafe,
		Out:                   os.Stdout,
	})
	defer func() {
		_ = applier.Close()
	}()

	statements := applier.ParseStatements(string(content))
	if len(statements) == 0 {
		fmt.Println("no SQL statements found in migration file")
		return nil
	}

	fmt.Printf("found %d statement(s) in %s\n", len(statements), flags.file)
	fmt.Println()

	preflight := applier.PreflightChecks(statements, flags.unsafe)

	if flags.dryRun {
		return applier.Apply(context.Background(), statements, preflight)
	}

	return executeApply(applier, statements, preflight, flags.timeout)
}

func executeApply(applier *apply.Applier, statements []string, preflight *apply.PreflightResult, timeout int) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
	defer cancel()

	fmt.Printf("connecting to database\n")
	if err := applier.Connect(ctx); err != nil {
		return err
	}
	defer func() {
		if err := applier.Close(); err != nil {
			fmt.Printf("failed to close database connection: %v\n", err)
		}
	}()

	return applier.Apply(ctx, statements, preflight)
}

// Helper functions
type parseResult struct {
	db  *core.Database
	err error
}

func parseSchemas(oldReader, newReader io.Reader) (*core.Database, *core.Database, error) {
	oldCh := make(chan parseResult, 1)
	newCh := make(chan parseResult, 1)

	go func() {
		oldData, err := io.ReadAll(oldReader)
		if err != nil {
			oldCh <- parseResult{nil, fmt.Errorf("failed to read old schema: %w", err)}
			return
		}

		p := parser.NewSQLParser()
		oldDB, err := p.ParseSchema(string(oldData))
		if err != nil {
			oldCh <- parseResult{nil, fmt.Errorf("failed to parse old schema: %w", err)}
			return
		}
		oldCh <- parseResult{oldDB, nil}
	}()

	go func() {
		newData, err := io.ReadAll(newReader)
		if err != nil {
			newCh <- parseResult{nil, fmt.Errorf("failed to read new schema: %w", err)}
			return
		}

		p := parser.NewSQLParser()
		newDB, err := p.ParseSchema(string(newData))
		if err != nil {
			newCh <- parseResult{nil, fmt.Errorf("failed to parse new schema: %w", err)}
			return
		}
		newCh <- parseResult{newDB, nil}
	}()

	oldResult := <-oldCh
	newResult := <-newCh

	if oldResult.err != nil {
		return nil, nil, oldResult.err
	}
	if newResult.err != nil {
		return nil, nil, newResult.err
	}

	return oldResult.db, newResult.db, nil
}

func validateDialect(dialectName string) error {
	supported := map[string]bool{"mysql": true}
	if !supported[strings.ToLower(dialectName)] {
		return fmt.Errorf("unsupported dialect: %s", dialectName)
	}
	return nil
}

func generateMigration(schemaDiff *diff.SchemaDiff, unsafe bool) (*migration.Migration, error) {
	d, err := dialect.GetDialect(dialect.MySQL)
	if err != nil {
		return nil, fmt.Errorf("generating migration: %w", err)
	}
	opts := dialect.DefaultMigrationOptions(dialect.MySQL)
	opts.IncludeUnsafe = unsafe
	return d.Generator().GenerateMigration(schemaDiff, opts), nil
}

func formatMigration(m *migration.Migration, format, outFile string) error {
	formatter, err := output.NewFormatter(format)
	if err != nil {
		return err
	}
	formatted, err := formatter.FormatMigration(m)
	if err != nil {
		return fmt.Errorf("failed to format output: %w", err)
	}
	return writeOutput(formatted, outFile, format)
}

func printInfo(format string, msg string) {
	if strings.EqualFold(strings.TrimSpace(format), string(output.FormatJSON)) {
		_, _ = fmt.Fprintln(os.Stderr, msg)
		return
	}
	fmt.Println(msg)
}

func writeOutput(content, outFile, format string) error {
	if outFile == "" {
		fmt.Print(content)
		return nil
	}

	if err := os.WriteFile(outFile, []byte(content), 0o644); err != nil {
		return fmt.Errorf("failed to write output: %w", err)
	}

	printInfo(format, fmt.Sprintf("Output saved to %s", outFile))
	return nil
}
