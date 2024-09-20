package s3package

import (
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

// Storage estructura para manejar el cliente de S3
type Storage struct {
	S3 *s3.S3
}

// NewS3Session inicializa una nueva sesión de S3 para MinIO o AWS S3
func NewS3Session(awskey, awssecret, awsregion, minio_endpoint string) Storage {
	minioSession := session.Must(session.NewSession(&aws.Config{
		Credentials:      credentials.NewStaticCredentials(awskey, awssecret, ""),
		Region:           aws.String(awsregion),
		Endpoint:         aws.String(minio_endpoint),
		DisableSSL:       aws.Bool(true),
		S3ForcePathStyle: aws.Bool(true),
	}))
	return Storage{
		S3: s3.New(minioSession),
	}
}

// UploadFile sube un archivo a S3 (o MinIO)
func (s *Storage) UploadFile(filePath, key, bucket_name string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", fmt.Errorf("error abriendo archivo: %w", err)
	}
	defer file.Close()

	uploader := s3manager.NewUploaderWithClient(s.S3)

	result, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(bucket_name),
		Key:    aws.String(key),
		Body:   file,
	})
	if err != nil {
		return "", fmt.Errorf("error subiendo archivo: %w", err)
	}

	return result.Location, nil
}

// DownloadFile descarga un archivo de S3 (o MinIO)
func (s *Storage) DownloadFile(key, downloadPath, bucket_name string) error {
	downloader := s3manager.NewDownloaderWithClient(s.S3)

	file, err := os.Create(downloadPath)
	if err != nil {
		return fmt.Errorf("error creando archivo de descarga: %w", err)
	}
	defer file.Close()

	_, err = downloader.Download(file, &s3.GetObjectInput{
		Bucket: aws.String(bucket_name),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("error descargando archivo: %w", err)
	}

	return nil
}

// ListFiles lista los archivos en el bucket de S3 (o MinIO)
func (s *Storage) ListFiles(bucket_name, key string) ([]*s3.Object, error) {
	result, err := s.S3.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket: aws.String(bucket_name),
		Prefix: aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("error listando archivos: %w", err)
	}

	return result.Contents, nil
}
