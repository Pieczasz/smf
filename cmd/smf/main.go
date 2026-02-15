// Package main contains the cli implementation of the tool. It uses cobra
// package for cli tool implementation.
package main

import (
	"os"

	"github.com/spf13/cobra"
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "smf",
		Short: "Schema migration framework – TOML-first database schema tool",
	}

	// rootCmd.AddCommand(migrationCmd())

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
