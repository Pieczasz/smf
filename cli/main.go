package main

import (
	"fmt"
	"os"
	"schemift/core"
	"schemift/dialect"
	"schemift/parser"
	"strings"

	"github.com/spf13/cobra"
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "schemift",
		Short: "Database migration tool",
	}

	var diffOutFile string
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

			schemaDiff := core.Diff(oldDB, newDB)
			if diffOutFile == "" {
				fmt.Print(schemaDiff.String())
				return nil
			}
			if err := schemaDiff.SaveToFile(diffOutFile); err != nil {
				return fmt.Errorf("failed to write output: %w", err)
			}
			fmt.Printf("Output saved to %s\n", diffOutFile)
			return nil
		},
	}

	diffCmd.Flags().StringVarP(&diffOutFile, "output", "o", "", "Output file for the diff")

	var fromDialect string
	var toDialect string
	var migrationOutFile string
	var rollbackOutFile string
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

			fmt.Printf("Migrating from %s (%s) to %s (%s)\n", oldPath, fromDialect, newPath, toDialect)

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

			schemaDiff := core.Diff(oldDB, newDB)
			fmt.Printf("Detected changes between schemas (old: %d tables, new: %d tables)\n",
				len(oldDB.Tables), len(newDB.Tables))

			d := dialect.NewMySQLDialect()
			opts := core.DefaultMigrationOptions(core.DialectMySQL)
			opts.IncludeUnsafe = unsafe
			migration := d.Generator().GenerateMigrationWithOptions(schemaDiff, opts)

			if migrationOutFile == "" {
				fmt.Print(migration.String())
				return nil
			}
			if err := migration.SaveToFile(migrationOutFile); err != nil {
				return fmt.Errorf("failed to write output: %w", err)
			}
			fmt.Printf("Output saved to %s\n", migrationOutFile)
			if rollbackOutFile != "" {
				if err := migration.SaveRollbackToFile(rollbackOutFile); err != nil {
					return fmt.Errorf("failed to write rollback output: %w", err)
				}
				fmt.Printf("Rollback saved to %s\n", rollbackOutFile)
			}
			return nil
		},
	}

	migrateCmd.Flags().StringVarP(&fromDialect, "from", "f", "mysql", "Source database dialect (e.g., mysql)")
	migrateCmd.Flags().StringVarP(&toDialect, "to", "t", "mysql", "Target database dialect (e.g., mysql)")
	migrateCmd.Flags().StringVarP(&migrationOutFile, "output", "o", "", "Output file for the generated migration SQL")
	migrateCmd.Flags().StringVarP(&rollbackOutFile, "rollback-output", "ro", "", "Output file for generated rollback SQL (run separately)")
	migrateCmd.Flags().BoolVarP(&unsafe, "unsafe", "u", false, "Generate unsafe migration (may drop/overwrite data); safe mode by default")

	//_ = migrateCmd.MarkFlagRequired("from")
	//_ = migrateCmd.MarkFlagRequired("to")

	rootCmd.AddCommand(diffCmd)
	rootCmd.AddCommand(migrateCmd)

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
