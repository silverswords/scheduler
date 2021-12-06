package cmd

import (
	"context"
	"errors"
	"os"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/silverswords/scheduler/pkg/config"
	"github.com/silverswords/scheduler/pkg/util"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(applyCmd)
}

var applyCmd = &cobra.Command{
	Use:   "apply",
	Short: "apply a config file",
	Args: func(cmd *cobra.Command, args []string) error {
		if len(args) < 1 {
			return errors.New("requires a yaml config")
		}

		return nil
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		configPath := args[0]
		data, err := os.ReadFile(configPath)
		if err != nil {
			return err
		}

		config, err := config.Unmarshal(data)
		if err != nil {
			return err
		}

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

		prefix, err := util.GetConfigPrefix()
		if err != nil {
			return err
		}

		if _, err := client.Put(context.TODO(), prefix+config.Name, string(data)); err != nil {
			return err
		}

		return nil
	},
}
