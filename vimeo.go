package main

 import (
         "fmt"
         "net/http"
         "os"
         "strconv"
         "io"
         "errors"
         "time"
         "crypto/md5"
         "strings"
         "flag"
 )

type FileInfo struct {
	size 			int
	rangeSupport 	bool
	md5 			string
}

var EOF = errors.New("EOF")

var NUMBER_OF_DOWNLOAD_THREADS = 1

var URL = "http://storage.googleapis.com/vimeo-test/work-at-vimeo.mp4"

var FILE_NAME = "video.mp4"

var downloadChannel = make(chan string)

func main() {
	parseInput()

	//Starting a timer to time the download.
	timerStart := time.Now()

	file, err := os.Create(FILE_NAME)

	if err != nil {
		fmt.Println("Error creating file: ", err)
		return;
	}

	defer file.Close()

	fileInfo, err := getFileInfoFromUrl(URL)
	if err != nil {
		fmt.Println("Error making request: ", err)
		return
	}

	fmt.Println("\nDownloading File Of Size: ",fileInfo.size," bytes")
	if fileInfo.rangeSupport == true {
		fmt.Println(" - Byte Ranges Supported")
		
		fmt.Println("\nDownloading Please Wait...")
		spinOffRangeDownloadsForFileWithInfo(file ,fileInfo)
	} else {
		fmt.Println(" - Byte Ranges Unsupported")

		fmt.Println("\nDownloading Please Wait...")
		downloadUrlToFileAtRange(URL, file, 0, int64(fileInfo.size), false)
	}

	elapsed := time.Since(timerStart)
	fmt.Println("Download Took:", elapsed);

	//Validate Integrity of file.
	md5Result, err := validateFileWithMD5(file, fileInfo.md5)
	if err !=nil {
		fmt.Println("Error Calculating md5", err);
	} else {
		fmt.Println("File Has Integrity =", md5Result);
	}
	
}

func parseInput() {
	//Parse input
    flag.Parse()
    s := flag.Arg(0)
    // string to int
    i, err := strconv.Atoi(s)
    NUMBER_OF_DOWNLOAD_THREADS = i;
    if err != nil {
        // handle error
        fmt.Println("usage: vimeo.go <# of threads>")
        os.Exit(2)
    }
}

func spinOffRangeDownloadsForFileWithInfo(file *os.File, fileInfo FileInfo) {
	rangeSize, finalRangeSize := calculateRangeSizeAndLastRange(fileInfo.size)

	for i := 0; i < NUMBER_OF_DOWNLOAD_THREADS; i++ {
		start := int64(i * rangeSize)
		finish := int64(int(start) + rangeSize)

		//Is this the last chunk?
		if i+1 == NUMBER_OF_DOWNLOAD_THREADS {
			finish = start + int64(finalRangeSize)	
		}
		
		go downloadUrlToFileAtRange(URL, file, start, finish, true)
	}

	//Wait for all download threads to complete.
	for i := 0; i < NUMBER_OF_DOWNLOAD_THREADS; i++ {
		msg := <-downloadChannel
		if msg == "error" {
			fmt.Println("Error Downloading File Chunk. Aborting...")
			return
		}
	}
}

func downloadUrlToFileAtRange(url string, file *os.File, start int64, end int64, useRange bool) {
	client := &http.Client{}

	bytesString := fmt.Sprintf("bytes=%d-%d", start, end)

	req, _ := http.NewRequest("GET", URL, nil)

	if useRange {
		req.Header.Set("Range", bytesString)
	}
	
	resp, err := client.Do(req)

	fmt.Println(resp.Status)

	if err != nil {
	    // handle error
	    downloadChannel <- "error"
	    fmt.Println(err)
	    return;
	}

	size, err := readFromReaderIntoFileAtOffset(resp.Body, file, start)

	if err == nil {
		fmt.Println("\nDownloaded File Chunk...")
		fmt.Println("Size:", size)
	}

	if useRange {
		downloadChannel <- "done"
	}	
}

func getFileInfoFromUrl(url string) (FileInfo, error) {
	client := &http.Client{}

	req, _ := http.NewRequest("GET", url, nil)
	resp, _ := client.Do(req)

	acceptRangesResult := resp.Header.Get("Accept-Ranges")
	contentLengthResult := resp.Header.Get("Content-Length")
	contentLengthResultInt, err := strconv.Atoi(contentLengthResult)
	md5 := resp.Header.Get("ETag");

	md5 = strings.Replace(md5, "\"", "", -1)

	if err != nil {
		return FileInfo{0, false, ""}, err
	} else {
		return FileInfo{contentLengthResultInt, "bytes" == acceptRangesResult, md5}, err
	}

	
}

func readFromReaderIntoFileAtOffset(reader io.Reader, file *os.File, offset int64) (written int64, err error) {
	buffer := make([]byte, 32*1024)
	totalSize := 0
	written = 0
	for {
		bytesRead, err := reader.Read(buffer)
		if bytesRead > 0 {
			nw, writeError := file.WriteAt(buffer[0:bytesRead], offset + written)
			if nw > 0 {
				written += int64(nw)
			}
			if writeError != nil {
				err = writeError
				break;
			}
		}
		if err == EOF {
			break
		}
		if err != nil {
			err = err
			break
		}
		totalSize = totalSize + bytesRead
	}

	return written, err
}


func calculateRangeSizeAndLastRange(fileSize int) (mainChunkSize int, lastChunchSize int) {
	mainChunkSize = fileSize/NUMBER_OF_DOWNLOAD_THREADS 
	lastChunchSize = mainChunkSize + fileSize % NUMBER_OF_DOWNLOAD_THREADS

	return mainChunkSize, lastChunchSize
}

func validateFileWithMD5(file *os.File, md5 string) (bool, error) {
	computedMD5, err := computeMd5(file)

	if err != nil {
		return false, err
	}

	return computedMD5 == md5, err
}

func computeMd5(file *os.File) (string, error) {
	var result []byte

  	hash := md5.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	byteHash := hash.Sum(result)

	return fmt.Sprintf("%x", byteHash), nil
}