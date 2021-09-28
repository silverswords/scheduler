package cmd

import (
	"context"
	"errors"
	"time"

	"github.com/silverswords/scheduler/pkg/util"
	"github.com/silverswords/scheduler/pkg/worker"
	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
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
		endpoints, err := util.GetEndpoints()
		if err != nil {
			return err
		}

		client, err := clientv3.New(clientv3.Config{
			Endpoints:   endpoints,
			DialTimeout: 5 * time.Second,
		})
		if err != nil {
			return err
		}

		worker := worker.New(args[0])
		return worker.Run(context.TODO(), client)
	},
}
