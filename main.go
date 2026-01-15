package main

import (
	"fmt"
	"os"
	"smf/dialect"
	"smf/dialect/mysql"
	"smf/diff"
	"smf/output"
	"smf/parser"
	"strings"

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
	diffCmd.Flags().StringVarP(&diffFormat, "format", "f", "human", "Output format: human or json")

	var fromDialect string
	var toDialect string
	var migrationOutFile string
	var rollbackOutFile string
	var migrationFormat string
	var unsafe bool

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
			opts.IncludeUnsafe = unsafe
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
	migrateCmd.Flags().StringVarP(&migrationFormat, "format", "f", "human", "Output format: human or json")
	migrateCmd.Flags().BoolVarP(&unsafe, "unsafe", "u", false, "Generate unsafe migration (may drop/overwrite data); safe mode by default")

	//_ = migrateCmd.MarkFlagRequired("from")
	//_ = migrateCmd.MarkFlagRequired("to")

	rootCmd.AddCommand(diffCmd)
	rootCmd.AddCommand(migrateCmd)

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
