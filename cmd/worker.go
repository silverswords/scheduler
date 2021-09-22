package cmd

import (
	"context"

	"github.com/silverswords/scheduler/pkg/util"
	"github.com/silverswords/scheduler/pkg/worker"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(workerCmd)
}

var workerCmd = &cobra.Command{
	Use:   "worker",
	Short: "start a worker",
	RunE: func(cmd *cobra.Command, args []string) error {
		endpoints, err := util.GetEndpoints()
		if err != nil {
			return err
		}
		worker := worker.New(args[0])
		worker.Run(context.TODO(), endpoints)
		return nil
	},
}
