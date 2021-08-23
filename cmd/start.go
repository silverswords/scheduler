package cmd

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/silverswords/scheduler/pkg/discover"
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

		for config := range discoverManager.SyncCh() {
			fmt.Println(config)
		}
		return nil
	},
}
