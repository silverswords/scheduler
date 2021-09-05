package cmd

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	errNoUploadHost = errors.New("can't find upload host in config file")
	errNoUploadPort = errors.New("can't find upload addr in config file")
)

func init() {
	rootCmd.AddCommand(uploadCmd)
}

var uploadCmd = &cobra.Command{
	Use:   "upload",
	Short: "upload a config executable file",
	Args: func(cmd *cobra.Command, args []string) error {
		if len(args) < 1 {
			return errors.New("requires a executable file")
		}

		return nil
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		fileKey := "file"
		filePath := args[0]

		url, err := parseConfig()
		if err != nil {
			return err
		}

		err = uploadFile(url, fileKey, filePath)

		return err
	},
}

func uploadFile(url, fileKey, filePath string) error {
	fileName := filepath.Base(filePath)
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	body := new(bytes.Buffer)
	writer := multipart.NewWriter(body)
	formFile, err := writer.CreateFormFile(fileKey, fileName)
	if err != nil {
		return err
	}
	_, err = io.Copy(formFile, file)
	if err != nil {
		return err
	}

	contentType := writer.FormDataContentType()
	writer.Close()

	err = writer.WriteField("fileName", fileName)
	if err != nil {
		return err
	}
	err = writer.WriteField("filePath", filePath)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", url, body)
	if err != nil {
		return err
	}

	req.Header.Add("Content-Type", contentType)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	fmt.Println(string(b))

	return nil
}

func parseConfig() (string, error) {
	uploadHost, ok := viper.Get("upload.host").(string)
	if !ok {
		return "", errNoUploadHost
	}
	uploadPort, ok := viper.Get("upload.port").(string)
	if !ok {
		return "", errNoUploadPort
	}

	return "http://" + uploadHost + ":" + uploadPort + "/upload", nil
}
