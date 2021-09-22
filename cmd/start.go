package cmd

import (
	"context"
	"time"

	"github.com/spf13/cobra"
	"go.etcd.io/etcd/clientv3"

	"github.com/silverswords/scheduler/pkg/config"
	"github.com/silverswords/scheduler/pkg/discover"
	scheduler "github.com/silverswords/scheduler/pkg/pool"
	"github.com/silverswords/scheduler/pkg/upload"
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
		defer client.Close()

		configDiscover := discover.NewManger("config/", func(value []byte) (interface{}, error) {
			return config.Unmarshal(value)
		})
		workerDiscover := discover.NewManger("workers/", func(value []byte) (interface{}, error) {
			return string(value), nil
		})

		go configDiscover.Run(context.Background(), client)
		go workerDiscover.Run(context.Background(), client)

		go upload.ListenAndServe()

		scheduler.New().Run(client, configDiscover.SyncCh(), workerDiscover.SyncCh())
		return nil
	},
}
