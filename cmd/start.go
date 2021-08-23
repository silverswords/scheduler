package cmd

import (
	"context"

	"github.com/spf13/cobra"

	"github.com/silverswords/scheduler/pkg/discover"
	"github.com/silverswords/scheduler/pkg/scheduler"
)

func init() {
	rootCmd.AddCommand(startCmd)
}

var startCmd = &cobra.Command{
	Use:   "start",
	Short: "start a scheduler",
	RunE: func(cmd *cobra.Command, args []string) error {
		discoverManager := discover.NewManger()
		go discoverManager.Run(context.Background())

		scheduler.New().Run(discoverManager.SyncCh())
		return nil
	},
}
