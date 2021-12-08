package cmd

import (
	"context"
	"errors"

	"github.com/silverswords/scheduler/pkg/api"
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
		endpoints, err := util.GetEndpoints()
		if err != nil {
			return err
		}

		configPath := args[0]
		client, err := api.NewClient(endpoints)
		if err != nil {
			return err
		}

		return client.ApplyConfig(context.Background(), configPath)
	},
}
