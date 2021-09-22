package server

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"

	"github.com/gin-gonic/gin"
	"github.com/silverswords/scheduler/pkg/model"
	"github.com/spf13/viper"
)

const (
	uploadServerAddr = "0.0.0.0:8080"
	localStorageDir  = "./files"
	bucketName       = "demo"
	location         = "buckets"
)

type Manager struct {
	router *gin.Engine
}

func NewManager() *Manager {
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

	return &Manager{
		router: gin.Default(),
	}
}

func ListenAndServe() {
	_, err := os.Stat("./logs")
	if err != nil {
		if os.IsNotExist(err) {
			err = os.MkdirAll("./logs", 0777)
			if err != nil {
				log.Fatal("Could not create log files")
			}
		}
	}

	logfile, err := os.Create("./logs/gin.log")
	if err != nil {
		log.Fatal("Could not create log file")
	}
	gin.ForceConsoleColor()
	gin.SetMode(gin.DebugMode)
	gin.DefaultWriter = io.MultiWriter(os.Stdout, logfile)

	err = NewManager().ListenAndServe()
	if err != nil {
		panic(err)
	}
}

func (m *Manager) ListenAndServe() error {
	m.router.POST("/upload", m.upload)

	return m.router.Run(uploadServerAddr)
}

func (m *Manager) upload(c *gin.Context) {
	// Set a lower memory limit for multipart forms (default is 32 MiB)
	m.router.MaxMultipartMemory = 8 << 20

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
		fmt.Printf("connect minio failed: %s", err.Error())
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
			fmt.Printf("delete local file failed: %s\n", err.Error())
		}
	}

	c.String(http.StatusOK, fmt.Sprintf("%d files uploaded!", len(files)))
}
