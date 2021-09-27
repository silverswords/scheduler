package cmd

import (
	"context"
	"errors"

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
	Args: func(cmd *cobra.Command, args []string) error {
		if len(args) < 1 {
			return errors.New("requires a worker name")
		}

		return nil
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		client, err := util.GetEtcdClient()
		if err != nil {
			return err
		}

		worker := worker.New(args[0])
		worker.Run(context.TODO(), client)
		return nil
	},
}
