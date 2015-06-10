package main

import (
	"flag"
	"io"
	"launchpad.net/goamz/aws"
	"launchpad.net/goamz/s3"
	"log"
	"mime"
	"os"
	"runtime"
	"strings"
	"sync"
)

// Threshold over which a file is uploaded in muliple parts
const MultipartThreshold = 2.5e+7

// Size of chunks to split a multipart upload into
const ChunkSize = 1e+7

type Target struct {
	bucket *s3.Bucket
	path   string
	acl    s3.ACL
}

func sendFiles(target Target, ch chan os.FileInfo) {
	for fi := range ch {
		// Recursive operation not supported
		if fi.IsDir() {
			continue
		}

		// Use multisend for large files
		if fi.Size() > MultipartThreshold {
			multiSend(target, fi)
		} else {
			singleSend(target, fi)
		}
	}
}

func singleSend(target Target, fi os.FileInfo) error {
	file, err := os.Open(fi.Name())
	if err != nil {
		return err
	}

	n := strings.Split(fi.Name(), ".")
	fileType := mime.TypeByExtension("." + n[len(n)-1])

	return target.bucket.PutReader(
		target.path,
		file,
		fi.Size(),
		fileType,
		target.acl)
}

func multiSend(target Target, fi os.FileInfo) error {

	var i int64

	file, err := os.Open(fi.Name())
	if err != nil {
		return err
	}

	var mult s3.Multi
	chans := make([]chan s3.Part, 0)

	for i = 0; i < fi.Size(); i += ChunkSize {

		chans = append(chans, make(chan s3.Part))

		go func(mult s3.Multi, file *os.File, pos int64, ch chan<- s3.Part) {

			reader := io.NewSectionReader(file, pos, ChunkSize)
			part, err := mult.PutPart(int(pos/ChunkSize), reader)
			if err != nil {
				mult.Abort()
			}
			ch <- part
			close(ch)

		}(mult, file, i, chans[len(chans)-1])
	}

	parts := make([]s3.Part, 0)

	for _, ch := range chans {
		p := <-ch
		parts = append(parts, p)
	}

	return mult.Complete(parts)

}

func finder(dir string, ch chan<- os.FileInfo) {
	defer close(ch)
	dh, err := os.Open(dir)
	if err != nil {
		log.Println(err)
		return
	}

	files, err := dh.Readdir(0)
	if err != nil {
		log.Println(err)
		return
	}

	for _, f := range files {
		ch <- f
	}
}

func main() {
	if os.Getenv("GOMAXPROCS") == "" {
		runtime.GOMAXPROCS(runtime.NumCPU())
	}

	var target Target

	dir := flag.String("d", ".", "Directory to upload")
	bucketName := flag.String("b", "", "Bucket name")
	flag.StringVar(&target.path, "p", "", "Bucket path")
	acl := flag.String("a", "private", "S3 ACL")
	regionName := flag.String("r", "", "Region")
	concurrency := flag.Int("c", 1, "Concurrency")

	flag.Parse()

	target.acl = s3.ACL(*acl)

	creds, err := aws.EnvAuth()
	if err != nil {
		log.Fatal(err)
	}

	region := aws.Region{Name: *regionName}
	conn := s3.New(creds, region)

	target.bucket = conn.Bucket(*bucketName)

	ch := make(chan os.FileInfo)

	finder(*dir, ch)

	var wg sync.WaitGroup

	for i := 0; i < *concurrency; i++ {
		wg.Add(1)

		go func(target Target, ch chan os.FileInfo) {
			defer wg.Done()
			sendFiles(target, ch)
		}(target, ch)
	}

	wg.Wait()

}
