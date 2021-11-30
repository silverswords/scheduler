package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"

	"github.com/gin-gonic/gin"
	"github.com/silverswords/scheduler/pkg/config"
	"github.com/silverswords/scheduler/pkg/model"
	"github.com/spf13/viper"
)

const (
	uploadServerAddr = "0.0.0.0:8080"
	localStorageDir  = "./files"
	bucketName       = "demo"
	location         = "buckets"
)

type manager struct {
	configCh chan map[string]*config.Config
}

func newManager(configCh chan map[string]*config.Config) *manager {
	return &manager{
		configCh: configCh,
	}
}

func Start(configCh chan map[string]*config.Config) {
	if err := logPrint(); err != nil {
		panic(err)
	}
	m := newManager(configCh)
	router := gin.Default()

	router.POST("/file/upload", m.upload)
	router.POST("/config/apply", m.apply)
	// router.POST("/config/delete", m.delete)
	// router.POST("/task/cancel", m.cancel)

	router.Run(uploadServerAddr)
}

func (m *manager) apply(c *gin.Context) {
	var req struct {
		Key    string `json:"key"`
		Config string `json:"config,omitempty"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		fmt.Println("[apply] bind params failed: ", err)
		c.String(http.StatusBadRequest, "server error")
		return
	}
	cf, err := config.Unmarshal([]byte(req.Config))
	if err != nil {
		c.String(http.StatusBadRequest, "server error")
		return
	}

	m.configCh <- map[string]*config.Config{
		req.Key: cf,
	}

	c.String(http.StatusOK, "apply succeed")
}

func (m *manager) delete(c *gin.Context) {
	var req struct {
		ConfigName string `json:"config_name,omitempty"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		fmt.Println("[delete] bind params failed: ", err)
		c.String(http.StatusBadRequest, "server error")
		return
	}

	// m.scheduler.DeleteConfig(req.ConfigName)

	// c.String(http.StatusOK)
}

func (m *manager) cancel(c *gin.Context) {
	var req struct {
		TaskName string `json:"task_name,omitempty"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		fmt.Println("[cancel] bind params failed: ", err)
		c.String(http.StatusBadRequest, "server error")
		return
	}

	// m.scheduler.CancelTask(req.TaskName)

	// c.String(http.StatusOK)
}

func logPrint() error {
	_, err := os.Stat("./logs")
	if err != nil {
		if os.IsNotExist(err) {
			err = os.MkdirAll("./logs", 0777)
			if err != nil {
				return errors.New("Could not create log files")
			}
		}
	}

	logfile, err := os.Create("./logs/gin.log")
	if err != nil {
		return errors.New("Could not create log file")
	}

	gin.ForceConsoleColor()
	gin.SetMode(gin.DebugMode)
	gin.DefaultWriter = io.MultiWriter(os.Stdout, logfile)

	return nil
}

func (m *manager) upload(c *gin.Context) {
	// Set a lower memory limit for multipart forms (default is 32 MiB)
	// m.router.MaxMultipartMemory = 8 << 20
	_, err := os.Stat("./files")
	if err != nil {
		if os.IsNotExist(err) {
			err = os.MkdirAll("./files", 0777)
			if err != nil {
				log.Fatal("Could not create files")
			}
			err = os.Chmod("./files", 0777)
			if err != nil {
				log.Fatal(err)
			}
		}
	}

	form, err := c.MultipartForm()
	if err != nil {
		c.String(http.StatusBadRequest, "upload failed: %v", err)
		return
	}

	files := form.File["file"]
	if len(files) == 0 {
		c.String(http.StatusBadRequest, "upload failed: %s", "please input right key or value")
		return
	}

	endpoint, ok := viper.Get("minio.endpoint").(string)
	if !ok {
		log.Println("[minio] error endpoint")
		c.String(http.StatusBadGateway, fmt.Sprintf("status: %d", http.StatusBadGateway))
		return
	}
	userName, ok := viper.Get("minio.user_name").(string)
	if !ok {
		log.Println("[minio] error user_name")
		c.String(http.StatusBadGateway, fmt.Sprintf("status: %d", http.StatusBadGateway))
		return
	}
	password, ok := viper.Get("minio.password").(string)
	if !ok {
		log.Println("[minio] error password")
		c.String(http.StatusBadGateway, fmt.Sprintf("status: %d", http.StatusBadGateway))
		return
	}

	for _, file := range files {
		err = c.SaveUploadedFile(file, localStorageDir+"/"+file.Filename)
		if err != nil {
			log.Println("local storage failed: ", err)
			c.String(http.StatusBadGateway, fmt.Sprintf("status: %d", http.StatusBadGateway))
			return
		}
		err = os.Chmod(localStorageDir+"/"+file.Filename, 0777)
		if err != nil {
			log.Println("file authorize failed: ", err)
			c.String(http.StatusBadGateway, fmt.Sprintf("status: %d", http.StatusBadGateway))
			return
		}
	}

	ctx := context.Background()

	minioManager := model.NewMinioManager()
	err = minioManager.Connect(endpoint, userName, password)
	if err != nil {
		log.Printf("connect minio failed: %s", err.Error())
		c.String(http.StatusBadGateway, fmt.Sprintf("status: %d", http.StatusBadGateway))
		return
	}
	for _, file := range files {
		f, err := os.Open(localStorageDir + "/" + file.Filename)
		if err != nil {
			log.Println("oepn file failed: ", err)
			c.String(http.StatusBadGateway, fmt.Sprintf("status: %d", http.StatusBadGateway))
			return
		}
		defer f.Close()

		buffer := make([]byte, 512)
		_, err = f.Read(buffer)
		if err != nil {
			log.Println("read file error: ", err)
			c.String(http.StatusBadGateway, fmt.Sprintf("status: %d", http.StatusBadGateway))
			return
		}
		contentType := http.DetectContentType(buffer)

		err = minioManager.Upload(ctx, bucketName, location, localStorageDir+"/"+file.Filename, file.Filename, contentType)
		if err != nil {
			log.Println("upload minio failed: ", err)
			c.String(http.StatusBadGateway, fmt.Sprintf("status: %d", http.StatusBadGateway))
			return
		}
		log.Printf("Successfully uploaded %s of size\n", file.Filename)

		err = os.RemoveAll(localStorageDir + "/" + file.Filename)
		if err != nil {
			log.Printf("delete local file failed: %s\n", err.Error())
		}
	}

	c.String(http.StatusOK, fmt.Sprintf("%d files uploaded!", len(files)))
}
