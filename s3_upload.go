package main

import (
	"bufio"
	"flag"
	"launchpad.net/goamz/aws"
	"launchpad.net/goamz/s3"
	"log"
	"os"
	"runtime"
	"sync"
)

// Threshold over which a file is uploaded in muliple parts
const MultipartThreshold = 2.5e+7

// Size of chunks to split a multipart upload into
const ChunkSize = 1e+7

type Target struct {
	bucket *s3.Bucket
	acl    s3.ACL
}

func sendFiles(target Target, ch chan os.FileInfo, dir string) {
	for fi := range ch {
		// Recursive operation not supported
		if fi.IsDir() {
			continue
		}

		file, err := os.Open(dir + "/" + fi.Name())
		if err != nil {
			log.Println(err)
			return
		}

		buf := bufio.NewReaderSize(file, 16384)

		err = target.bucket.PutReader(
			fi.Name(),
			buf,
			fi.Size(),
			"binary/octet-stream",
			target.acl)

		if err != nil {
			log.Println(err)
			return
		}
		log.Println("Completed", fi.Name())
		return
	}
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

	log.Println("Found", len(files), "files")

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
	acl := flag.String("a", "private", "S3 ACL")
	concurrency := flag.Int("c", 1, "Concurrency")

	flag.Parse()

	target.acl = s3.ACL(*acl)

	creds, err := aws.EnvAuth()
	if err != nil {
		log.Fatal(err)
	}

	region := aws.EUWest
	conn := s3.New(creds, region)

	target.bucket = conn.Bucket(*bucketName)

	ch := make(chan os.FileInfo)

	go finder(*dir, ch)

	var wg sync.WaitGroup

	for i := 0; i < *concurrency; i++ {
		wg.Add(1)

		go func(target Target, ch chan os.FileInfo, dir string) {
			defer wg.Done()
			sendFiles(target, ch, dir)
		}(target, ch, *dir)
	}

	wg.Wait()

}
