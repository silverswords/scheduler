package cmd

import (
	"context"
	"errors"
	"log"
	"net"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"

	taskspb "github.com/silverswords/scheduler/api/tasks"
	"github.com/silverswords/scheduler/pkg/api"
	"github.com/silverswords/scheduler/pkg/config"
	"github.com/silverswords/scheduler/pkg/discover"
	"github.com/silverswords/scheduler/pkg/pool"
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

		client, err := api.NewClient(endpoints)
		if err != nil {
			return err
		}

		configDiscover := discover.NewManger(client.ConfigPrefix(), func(value []byte) (interface{}, error) {
			return config.Unmarshal(value)
		})

		workerDiscover := discover.NewManger(client.WorkerPrefix(), func(value []byte) (interface{}, error) {
			config, err := worker.Unmarshal(value)
			if err != nil {
				return nil, err
			}
			return config, nil
		})

		var g util.Group
		scheduler := pool.New()
		configContext, configCancleFunc := context.WithCancel(context.Background())
		workerContext, workerCancleFunc := context.WithCancel(context.Background())

		grpcServer := grpc.NewServer()

		g.Add(func() error {
			return configDiscover.Run(configContext, client.GetOriginClient())
		}, func(err error) {
			log.Printf("config discover: %s\n", err)
			configCancleFunc()
		})

		g.Add(func() error {
			return workerDiscover.Run(workerContext, client.GetOriginClient())
		}, func(err error) {
			log.Printf("worker discover: %s\n", err)
			workerCancleFunc()
		})

		g.Add(func() error {
			// server.Start(configCh)
			// return errors.New("server exit")
			for {
			}
		}, func(err error) {})

		g.Add(func() error {
			scheduler.Run(client, configDiscover.SyncCh(), workerDiscover.SyncCh())
			return errors.New("scheduler pool exit")
		}, func(err error) {
			scheduler.Stop()
		})

		g.Add(func() error {
			addr := viper.Get("grpc.addr").(string)
			l, err := net.Listen("tcp", addr)
			if err != nil {
				return err
			}

			taskspb.RegisterStateChangeServer(grpcServer, scheduler)

			return grpcServer.Serve(l)
		}, func(err error) {
			grpcServer.Stop()
		})

		return g.Run()
	},
}
