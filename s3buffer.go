package s3buffer

import (
	"bytes"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"io"
	"os"
	"time"
)

const (
	maxSize = 100 * 1000000
)

type Buffer struct {
	Name    string
	MaxSize int
	Bucket  string
	buffer  *bytes.Buffer
}

func NewBuffer(name, bucket string) *Buffer {
	return &Buffer{
		Name:    name,
		Bucket:  bucket,
		MaxSize: maxSize,
		buffer:  new(bytes.Buffer),
	}
}

func (b *Buffer) WriteLine(line string) {
	b.Write([]byte(line + "\n"))
}

func (b *Buffer) Write(data []byte) (int, error) {
	n, err := b.buffer.Write(data)
	b.checkFlush()

	return n, err
}

func (b *Buffer) checkFlush() {
	if b.ShouldFlush() {
		b.Flush()
	}
}

func (b *Buffer) ShouldFlush() bool {
	return b.buffer.Len() >= b.MaxSize
}

func (b *Buffer) Flush() {
	stamp := time.Now().Unix()
	name := fmt.Sprintf("%v/%v", b.Name, stamp)

	reader := bytes.NewReader(b.buffer.Bytes())
	b.buffer = new(bytes.Buffer)

	go b.upload(name, reader)
}

func (b *Buffer) upload(name string, buffer io.Reader) {
	svc := session.New(aws.NewConfig().WithMaxRetries(10))

	uploader := s3manager.NewUploader(svc)

	upParams := &s3manager.UploadInput{
		Bucket: aws.String(b.Bucket),
		Key:    aws.String(name),
		Body:   buffer,
	}

	_, err := uploader.Upload(upParams)

	if err != nil {
		fmt.Printf("error %s\n", err)
		os.Exit(1)
	}
}
