package main

import (
	"fmt"
	"os"

	"schemift/internal/parser"

	"github.com/spf13/cobra"
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "schemift",
		Short: "Database migration tool",
	}

	parseCmd := &cobra.Command{
		Use:   "parse <schema.sql>",
		Short: "Parse and display schema",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			data, err := os.ReadFile(args[0])
			if err != nil {
				return fmt.Errorf("failed to read file: %w", err)
			}

			p := parser.NewSQLParser()
			db, err := p.ParseSchema(string(data))
			if err != nil {
				return fmt.Errorf("parse error: %w", err)
			}

			fmt.Printf("Tables found: %d\n", len(db.Tables))
			for _, t := range db.Tables {
				fmt.Printf("- %s (%d columns)\n", t.Name, len(t.Columns))
				for _, c := range t.Columns {
					fmt.Printf("  - %s: %s\n", c.Name, c.TypeRaw)
				}
			}
			return nil
		},
	}

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

			// TODO: call core.Diff(oldDB, newDB)
			fmt.Printf("Old DB: %d tables\n", len(oldDB.Tables))
			fmt.Printf("New DB: %d tables\n", len(newDB.Tables))
			return nil
		},
	}

	rootCmd.AddCommand(parseCmd)
	rootCmd.AddCommand(diffCmd)

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
