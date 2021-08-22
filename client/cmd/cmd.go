package cmd

import (
	"github.com/spf13/cobra"
)

var (
	rootCmd = &cobra.Command{
		Use:   "nimitz",
		Short: "a scheduler for mini job",
		Long:  `nimitz is a schedule for mini job.`,
	}
)

// Execute executes the root command.
func Execute() error {
	return rootCmd.Execute()
}
