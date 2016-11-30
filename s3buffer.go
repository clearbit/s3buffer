package s3buffer

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/twinj/uuid"
	"os"
	"log"
)

const (
	maxSize = 20 * 1000000
)

type Buffer struct {
	Name    string
	MaxSize int64
	Bucket  string
	Header  string
	buffer  *bytes.Buffer
	svc *session.Session
	uploader *s3manager.Uploader
}

func NewBuffer(name, bucket, header string) *Buffer {
	svc := session.New(aws.NewConfig().WithMaxRetries(10))
	uploader := s3manager.NewUploader(svc)

	buffer := &Buffer{
		Name:     name,
		Bucket:   bucket,
		Header:   header,
		MaxSize:  maxSize,
		svc:      svc,
		uploader: uploader,
	}

	buffer.reset()

	return buffer
}

func (b *Buffer) reset() {
	b.buffer = new(bytes.Buffer)

	if b.Header != "" {
		b.buffer.WriteString(b.Header)
	}
}

func (b *Buffer) WriteString(str string) {
	b.buffer.WriteString(str)
}

func (b *Buffer) WriteLine(line string) {
	b.WriteString(line + "\n")
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
	name := fmt.Sprintf("%v/%v", b.Name, uuid.NewV4())
	b.upload(name)
	b.reset()
}

func (b *Buffer) upload(name string) {
	reader := bytes.NewReader(b.buffer.Bytes())

	upParams := &s3manager.UploadInput{
		Bucket: aws.String(b.Bucket),
		Key:    aws.String(name),
		Body:   reader,
	}

	_, err := b.uploader.Upload(upParams)

	if err != nil {
		log.Fatal("error %v\n", err)
	}
}
