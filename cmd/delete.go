package cmd

import (
	"context"
	"errors"

	commandpb "github.com/silverswords/scheduler/api/scheduler/cmd"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
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
		configName := args[0]
		// data, err := os.ReadFile(configPath)
		// if err != nil {
		// 	return err
		// }

		// config, err := config.Unmarshal(data)
		// if err != nil {
		// 	return err
		// }

		// url, err := util.GetURL()
		// if err != nil {
		// 	return err
		// }

		conn, err := grpc.Dial("192.168.0.21:8000", grpc.WithInsecure(), grpc.WithBlock())
		if err != nil {
			return err
		}
		defer conn.Close()
		c := commandpb.NewCommandClient(conn)

		_, err = c.DeleteConfig(context.Background(), &commandpb.DeleteRequest{ConfigName: configName})

		return err
		// body := fmt.Sprintf(`{
		// 	"config_name":"%s"
		// 	}`, config.Name)

		// req, err := http.NewRequest("POST", url+deletePath, bytes.NewBuffer([]byte(body)))
		// if err != nil {
		// 	return err
		// }
		// client := &http.Client{}
		// _, err = client.Do(req)
		// if err != nil {
		// 	return err
		// }

		// return nil
	},
}
