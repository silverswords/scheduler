package cmd

import (
	"errors"
	"fmt"

	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(pusherCmd)
}

var pusherCmd = &cobra.Command{
	Use:   "pusher",
	Short: "get message pusher subscribe address",
	Args: func(cmd *cobra.Command, args []string) error {
		if len(args) < 1 {
			return errors.New("requires pusher way")
		}

		return nil
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		switch args[0] {
		case "wx":
			fmt.Println("//wxpusher.zjiecode.com/api/qrcode/IDfNy0EYxmfbZfyVDVuKfURdIZX8DoHEjtqQxliB1pGrkPYCIpUtEgoR6thLcuwS.jpg")
		case "email":
			// modify .niimtz config email
		default:
			fmt.Println("only support {wx} way")
		}

		return nil
	},
}
