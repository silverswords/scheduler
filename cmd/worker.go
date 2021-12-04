package cmd

import (
	"context"
	"errors"
	"os"
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
			return errors.New("requires a worker config file")
		}

		return nil
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		configPath := args[0]
		data, err := os.ReadFile(configPath)
		if err != nil {
			return err
		}

		config, err := pool.Unmarshal(data)
		if err != nil {
			return err
		}
		conn, err := grpc.Dial("192.168.0.21:8000", grpc.WithInsecure(), grpc.WithBlock())
		if err != nil {
			return err
		}
		defer conn.Close()
		c := registrypb.NewRegistryClient(conn)

		_, err = c.Regist(context.Background(), &registrypb.RegistryRequest{WorkerAddr: , Labels: })

		

		addr := viper.Get("grpc.addr").(string)
			l, err := net.Listen("tcp", addr)
			if err != nil {
				return err
			}

			registrypb.RegisterRegistryServer(grpcServer, scheduler)
			taskspb.RegisterTasksServer(grpcServer, scheduler)
			commandpb.RegisterCommandServer(grpcServer, scheduler)

			return grpcServer.Serve(l)

		// endpoints, err := util.GetEndpoints()
		// if err != nil {
		// 	return err
		// }

		// client, err := clientv3.New(clientv3.Config{
		// 	Endpoints:   endpoints,
		// 	DialTimeout: 5 * time.Second,
		// })
		// if err != nil {
		// 	return err
		// }

		// worker, err := worker.New(config)
		// if err != nil {
		// 	return err
		// }

		// return worker.Run(context.TODO(), client)
	},
}
