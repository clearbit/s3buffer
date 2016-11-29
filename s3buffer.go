package s3buffer

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/twinj/uuid"
	"os"
	"io/ioutil"
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
	tmpfile *os.File
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
	if b.tmpfile != nil {
		log.Println("Removing %v", b.tmpfile.Name())

		b.tmpfile.Close()
		err := os.Remove(b.tmpfile.Name())

		if err != nil {
			log.Fatal(err)
		}
	}

	var err error
	b.tmpfile, err = ioutil.TempFile("", "s3buffer")

	if err != nil {
		log.Fatal(err)
	}

	if b.Header != "" {
		b.tmpfile.WriteString(b.Header)
	}
}

func (b *Buffer) WriteString(str string) {
	b.tmpfile.WriteString(str)
}

func (b *Buffer) WriteLine(line string) {
	b.WriteString(line + "\n")
}

func (b *Buffer) Write(data []byte) (int, error) {
	n, err := b.tmpfile.Write(data)
	b.checkFlush()

	return n, err
}

func (b *Buffer) checkFlush() {
	if b.ShouldFlush() {
		b.Flush()
	}
}

func (b *Buffer) ShouldFlush() bool {
	return b.Len() >= b.MaxSize
}

func (b *Buffer) Len() int64 {
	fi, err := b.tmpfile.Stat()

	if err != nil {
		log.Fatal(err)
	}

	return fi.Size()
}

func (b *Buffer) Flush() {
	name := fmt.Sprintf("%v/%v", b.Name, uuid.NewV4())
	b.upload(name)
	b.reset()
}

func (b *Buffer) upload(name string) {
	upParams := &s3manager.UploadInput{
		Bucket: aws.String(b.Bucket),
		Key:    aws.String(name),
		Body:   b.tmpfile,
	}

	_, err := b.uploader.Upload(upParams)

	if err != nil {
		log.Fatal("error %v\n", err)
	}
}
