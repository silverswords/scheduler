package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/silverswords/scheduler/pkg/config"
	"github.com/spf13/cobra"
	"go.etcd.io/etcd/clientv3"
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

		if err := config.Validate(data); err != nil {
			return err
		}

		client, err := clientv3.New(clientv3.Config{
			Endpoints:   []string{"localhost:60428"},
			DialTimeout: 5 * time.Second,
		})
		if err != nil {
			return err
		}
		defer client.Close()

		response, err := client.Put(context.TODO(), "config/"+configPath, string(data))
		if err != nil {
			return err
		}

		fmt.Println(response)
		return nil
	},
}
