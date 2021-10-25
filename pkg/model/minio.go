package model

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type Manager struct {
	minioClient *minio.Client
}

func NewMinioManager() *Manager {
	return &Manager{}
}

func (m *Manager) Connect(endpoint, userName, password string) error {
	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(userName, password, ""),
		Secure: false,
	})
	if err != nil {
		return fmt.Errorf("create minioClient error: %s\n", err.Error())
	}

	m.minioClient = minioClient

	return nil
}

func (m *Manager) ConnectWithSSL(endpoint, userName, password string) error {
	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(userName, password, ""),
		Secure: true,
	})
	if err != nil {
		return fmt.Errorf("create minioClient error: %s\n", err.Error())
	}

	m.minioClient = minioClient

	return nil
}

func (m *Manager) Upload(ctx context.Context, bucketName, location, filePath, fileName, fileType string) error {
	err := m.minioClient.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{Region: location})
	if err != nil {
		// Check to see if we already own this bucket (which happens if you run this twice)
		exists, errBucketExists := m.minioClient.BucketExists(ctx, bucketName)
		if errBucketExists == nil && exists {
			log.Printf("We already own %s\n", bucketName)
			return nil
		} else {
			return fmt.Errorf("create bucket failed: %s\n", err.Error())
		}
	} else {
		log.Printf("Successfully created %s\n", bucketName)
	}

	_, err = m.minioClient.FPutObject(ctx, bucketName, fileName, filePath, minio.PutObjectOptions{ContentType: fileType})
	if err != nil {
		return fmt.Errorf("upload minio failed: %s\n", err.Error())
	}

	return nil
}

func (m *Manager) Download(ctx context.Context, bucketName, location, filePath, fileName string) error {
	err := m.minioClient.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{Region: location})
	if err != nil {
		// Check to see if we already own this bucket (which happens if you run this twice)
		exists, errBucketExists := m.minioClient.BucketExists(ctx, bucketName)
		if errBucketExists == nil && exists {
			return fmt.Errorf("We already own %s\n", bucketName)
		} else {
			return fmt.Errorf("create bucket failed: %s\n", err.Error())
		}
	} else {
		log.Printf("Successfully created %s\n", bucketName)
	}

	err = m.minioClient.FGetObject(ctx, bucketName, fileName, filePath, minio.GetObjectOptions{})
	if err != nil {
		return err
	}

	err = os.Chmod(filePath, 0777)
	if err != nil {
		return err
	}

	return nil
}
