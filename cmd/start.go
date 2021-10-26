package cmd

import (
	"context"
	"log"
	"time"

	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/silverswords/scheduler/pkg/config"
	"github.com/silverswords/scheduler/pkg/discover"
	"github.com/silverswords/scheduler/pkg/pool"
	"github.com/silverswords/scheduler/pkg/server"
	"github.com/silverswords/scheduler/pkg/util"
	"github.com/silverswords/scheduler/pkg/worker"
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
			config, err := worker.Unmarshal(value)
			if err != nil {
				return nil, err
			}
			return config.Lables, nil
		})
		var g util.Group
		scheduler := pool.New()
		configContext, configCancleFunc := context.WithCancel(context.Background())
		workerContext, workerCancleFunc := context.WithCancel(context.Background())

		g.Add(func() error {
			return configDiscover.Run(configContext, client)
		}, func(err error) {
			log.Printf("config discover: %s\n", err)
			configCancleFunc()
		})

		g.Add(func() error {
			return workerDiscover.Run(workerContext, client)
		}, func(err error) {
			log.Printf("worker discover: %s\n", err)
			workerCancleFunc()
		})

		g.Add(func() error {
			server.ListenAndServe()
			return nil
		}, func(err error) {})

		g.Add(func() error {
			scheduler.Run(client, configDiscover.SyncCh(), workerDiscover.SyncCh())
			return nil
		}, func(err error) {
			scheduler.Stop()
		})

		return g.Run()
	},
}
