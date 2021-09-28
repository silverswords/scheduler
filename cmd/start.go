package cmd

import (
	"context"
	"time"

	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/silverswords/scheduler/pkg/config"
	"github.com/silverswords/scheduler/pkg/discover"
	scheduler "github.com/silverswords/scheduler/pkg/pool"
	"github.com/silverswords/scheduler/pkg/server"
	"github.com/silverswords/scheduler/pkg/util"
)

func init() {
	rootCmd.AddCommand(startCmd)
}

var startCmd = &cobra.Command{
	Use:   "start",
	Short: "start a scheduler",
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

		configPrefix, err := util.GetConfigPrefix()
		if err != nil {
			return err
		}

		workerPrefix, err := util.GetWorkerDiscoverPrefix()
		if err != nil {
			return err
		}

		configDiscover := discover.NewManger(configPrefix, func(value []byte) (interface{}, error) {
			return config.Unmarshal(value)
		})

		workerDiscover := discover.NewManger(workerPrefix, func(value []byte) (interface{}, error) {
			return string(value), nil
		})

		go configDiscover.Run(context.Background(), client)
		go workerDiscover.Run(context.Background(), client)

		go server.ListenAndServe()

		scheduler.New().Run(client, configDiscover.SyncCh(), workerDiscover.SyncCh())
		return nil
	},
}
