package cmd

import (
	"bytes"
	"errors"
	"fmt"
	"net/http"
	"os"

	"github.com/silverswords/scheduler/pkg/config"
	"github.com/silverswords/scheduler/pkg/util"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(deleteCmd)
}

var deleteCmd = &cobra.Command{
	Use:   "delete",
	Short: "delete a config file",
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

		url, err := util.GetURL()
		if err != nil {
			return err
		}

		body := fmt.Sprintf(`{
			"config_name":"%s"
			}`, config.Name)

		req, err := http.NewRequest("POST", url+deletePath, bytes.NewBuffer([]byte(body)))
		if err != nil {
			return err
		}
		client := &http.Client{}
		_, err = client.Do(req)
		if err != nil {
			return err
		}

		return nil
	},
}
