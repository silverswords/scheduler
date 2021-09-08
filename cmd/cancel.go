package cmd

import (
	"context"
	"errors"
	"os"
	"time"

	taskConfig "github.com/silverswords/scheduler/pkg/config"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.etcd.io/etcd/clientv3"
)

func init() {
	rootCmd.AddCommand(cancelCmd)
}

var cancelCmd = &cobra.Command{
	Use:   "remove",
	Short: "remove config file",
	Args: func(cmd *cobra.Command, args []string) error {
		if len(args) < 1 {
			return errors.New("requires a yaml config")
		}

		return nil
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		for _, configPath := range args {
			data, err := os.ReadFile(configPath)
			if err != nil {
				return err
			}

			if err := taskConfig.Validate(data); err != nil {
				return err
			}

			endpoints, ok := viper.Get("etcd.endpoints").([]interface{})
			if !ok {
				return errNoEndpoints
			}

			eps := make([]string, 0)
			for _, endpoint := range endpoints {
				endpoint, ok := endpoint.(string)
				if !ok {
					return errWrongEndpoint
				}

				eps = append(eps, endpoint)
			}
			client, err := clientv3.New(clientv3.Config{
				Endpoints:   eps,
				DialTimeout: 5 * time.Second,
			})
			if err != nil {
				return err
			}
			defer client.Close()

			if _, err := client.Put(context.TODO(), "config remove"+configPath, string(data)); err != nil {
				return err
			}
		}

		return nil
	},
}