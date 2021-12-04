package cmd

import (
	"net"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"

	commandpb "github.com/silverswords/scheduler/api/scheduler/cmd"
	registrypb "github.com/silverswords/scheduler/api/scheduler/registry"
	taskspb "github.com/silverswords/scheduler/api/tasks"
	"github.com/silverswords/scheduler/pkg/api"
	"github.com/silverswords/scheduler/pkg/pool"
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

		configPrefix, err := util.GetConfigPrefix()
		if err != nil {
			return err
		}

		workerPrefix, err := util.GetWorkerDiscoverPrefix()
		if err != nil {
			return err
		}

		taskPrefix, err := util.GetTaskDispatchPrefix()
		if err != nil {
			return err
		}

		client, err := api.NewClient(endpoints, configPrefix, taskPrefix, workerPrefix)
		if err != nil {
			return err
		}

		var g util.Group
		scheduler := pool.New()

		grpcServer := grpc.NewServer()

		// g.Add(func() error {
		// 	server.Start(configCh)
		// 	return nil
		// }, func(err error) {})

		g.Add(func() error {
			scheduler.Run(client)
			return nil
		}, func(err error) {
			scheduler.Stop()
		})

		g.Add(func() error {
			addr := viper.Get("grpc.addr").(string)
			l, err := net.Listen("tcp", addr)
			if err != nil {
				return err
			}

			registrypb.RegisterRegistryServer(grpcServer, scheduler)
			taskspb.RegisterTasksServer(grpcServer, scheduler)
			commandpb.RegisterCommandServer(grpcServer, scheduler)

			return grpcServer.Serve(l)
		}, func(err error) {
			grpcServer.Stop()
		})

		return g.Run()
	},
}
