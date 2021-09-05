package upload

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"

	"github.com/gin-gonic/gin"
)

type Manager struct {
	router      *gin.Engine
	destination string
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
		router:      gin.Default(),
		destination: "./files",
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

	m := NewManager()

	m.ListenAndServe()
}

func (m *Manager) ListenAndServe() {
	m.router.POST("/upload", m.upload)

	m.router.Run(":8080")
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

	for _, file := range files {
		log.Println(file.Filename)

		// Upload the file to specific dst.
		c.SaveUploadedFile(file, m.destination+"/"+file.Filename)
		err = os.Chmod(m.destination+"/"+file.Filename, 0777)
		if err != nil {
			log.Println(err)
		}
	}

	c.String(http.StatusOK, fmt.Sprintf("%d files uploaded!", len(files)))
}
