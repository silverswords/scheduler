package cmd

import (
	"context"

	"github.com/spf13/cobra"

	"github.com/silverswords/scheduler/pkg/config"
	"github.com/silverswords/scheduler/pkg/discover"
	scheduler "github.com/silverswords/scheduler/pkg/pool"
	"github.com/silverswords/scheduler/pkg/server"
	"github.com/silverswords/scheduler/pkg/util"
)

func init() {
	rootCmd.AddCommand(startCmd)
}

var startCmd = &cobra.Command{
	Use:   "start",
	Short: "start a scheduler",
	RunE: func(cmd *cobra.Command, args []string) error {
		client, err := util.GetEtcdClient()
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

		go server.ListenAndServe()

		scheduler.New().Run(client, configDiscover.SyncCh(), workerDiscover.SyncCh())
		return nil
	},
}
