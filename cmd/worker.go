package cmd

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"

	registrypb "github.com/silverswords/scheduler/api/scheduler/registry"
	workerpb "github.com/silverswords/scheduler/api/worker"
	"github.com/silverswords/scheduler/pkg/pool"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
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
		conn, err := grpc.Dial("192.168.0.21:8000", grpc.WithInsecure())
		if err != nil {
			return err
		}
		defer conn.Close()
		c := registrypb.NewRegistryClient(conn)

		labels := strings.Join(config.Labels, "/")
		fmt.Println(labels, "labels")
		_, err = c.Regist(context.Background(), &registrypb.Request{WorkerAddr: "192.168.0.21:8000", Labels: labels})
		if err != nil {
			return err
		}
		l, err := net.Listen("tcp", "192.168.0.21:4000")
		if err != nil {
			return err
		}

		grpcServer := grpc.NewServer()
		worker, err := pool.NewWorker()
		if err != nil {
			return err
		}
		workerpb.RegisterWorkerServer(grpcServer, worker)

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
