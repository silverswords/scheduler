package cmd

import (
	"context"
	"errors"

	commandpb "github.com/silverswords/scheduler/api/scheduler/cmd"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

func init() {
	rootCmd.AddCommand(cancelCmd)
}

var cancelCmd = &cobra.Command{
	Use:   "cancel",
	Short: "cancel a task",
	Args: func(cmd *cobra.Command, args []string) error {
		if len(args) < 1 {
			return errors.New("requires a yaml config")
		}

		return nil
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		taskName := args[0]

		// config, err := config.Unmarshal(data)
		// if err != nil {
		// 	return err
		// }

		conn, err := grpc.Dial("192.168.0.21:8000", grpc.WithInsecure(), grpc.WithBlock())
		if err != nil {
			return err
		}
		defer conn.Close()
		c := commandpb.NewCommandClient(conn)

		_, err = c.CancelTask(context.Background(), &commandpb.CancelRequest{TaskName: taskName})

		return err
		// body := fmt.Sprintf(`{
		// 	"task_name":"%s"
		// 	}`, config.Name)

		// req, err := http.NewRequest("POST", url+cancelPath, bytes.NewBuffer([]byte(body)))
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
