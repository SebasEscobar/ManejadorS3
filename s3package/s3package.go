package s3package

import (
	"context"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// S3Client estructura para manejar el cliente de S3
type S3Client struct {
	client *s3.Client
	bucket string
	region string
}

// NewS3Client inicializa un nuevo cliente S3
func NewS3Client(bucket, region string) (*S3Client, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("error cargando configuración: %w", err)
	}

	client := s3.NewFromConfig(cfg)

	return &S3Client{
		client: client,
		bucket: bucket,
		region: region,
	}, nil
}

// UploadFile sube un archivo a S3
func (s *S3Client) UploadFile(filePath, key string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", fmt.Errorf("error abriendo archivo: %w", err)
	}
	defer file.Close()

	uploader := manager.NewUploader(s.client)

	result, err := uploader.Upload(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Body:   file,
	})
	if err != nil {
		return "", fmt.Errorf("error subiendo archivo: %w", err)
	}

	return result.Location, nil
}

// DownloadFile descarga un archivo de S3
func (s *S3Client) DownloadFile(key, downloadPath string) error {
	downloader := manager.NewDownloader(s.client)

	file, err := os.Create(downloadPath)
	if err != nil {
		return fmt.Errorf("error creando archivo de descarga: %w", err)
	}
	defer file.Close()

	_, err = downloader.Download(context.TODO(), file, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("error descargando archivo: %w", err)
	}

	return nil
}

// ListFiles lista los archivos en el bucket de S3
func (s *S3Client) ListFiles() ([]types.Object, error) {
	result, err := s.client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
	})
	if err != nil {
		return nil, fmt.Errorf("error listando archivos: %w", err)
	}

	return result.Contents, nil
}
