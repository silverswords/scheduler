package cmd

import (
	"context"
	"errors"
	"os"

	commandpb "github.com/silverswords/scheduler/api/scheduler/cmd"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
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

		// addr, err := util.GetServerAddr()
		// if err != nil {
		// 	return err
		// }

		// port, err := util.GetServerPort()
		// if err != nil {
		// 	return err
		// }

		conn, err := grpc.Dial("192.168.0.21:8000", grpc.WithInsecure(), grpc.WithBlock())
		if err != nil {
			return err
		}
		defer conn.Close()
		c := commandpb.NewCommandClient(conn)

		_, err = c.ApplyConfig(context.Background(), &commandpb.ApplyRequest{ConfigName: data})

		return err
		// body := fmt.Sprintf(`{
		// 	"key":"%s",
		// 	"config":"%s"
		// 	}`, config.Name, string(data))

		// req, err := http.NewRequest("POST", addr+":"+port+applyPath, bytes.NewBuffer([]byte(body)))
		// if err != nil {
		// 	return err
		// }
		// client := &http.Client{}
		// _, err = client.Do(req)
		// if err != nil {
		// 	return err
		// }
	},
}
