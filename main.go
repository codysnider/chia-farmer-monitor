package main

import (
	"fmt"
	"github.com/joho/godotenv"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

var fileWriteCooldownSeconds int
var movingFilesMutex = &sync.Mutex{}
var movingFiles []string
var transferSuffix string

func srcIsMoving(src string) bool {
	for _, movingSrc := range movingFiles {
		if movingSrc == src {
			return true
		}
	}
	return false
}

func monitorForTransferFiles(srcPath, dstPath string) {

	srcPathAbsolute, srcPathErr := filepath.Abs(srcPath)
	if srcPathErr != nil {
		log.Fatalln(srcPathErr)
	}

	dstPathAbsolute, dstPathErr := filepath.Abs(dstPath)
	if dstPathErr != nil {
		log.Fatalln(dstPathErr)
	}

	log.Printf("monitoring for files: %s -> %s\n", srcPathAbsolute, dstPathAbsolute)

	for {

		files, err := ioutil.ReadDir(srcPath)
		if err != nil {
			log.Fatal(err)
		}

		for _, file := range files {
			if filepath.Ext(file.Name()) == transferSuffix {

				// Get clean absolute path for source file
				srcFilename := filepath.Clean(file.Name())
				src := fmt.Sprintf("%s/%s", srcPathAbsolute, srcFilename)

				// Get fileinfo to check modified time
				srcFileinfo, srcFileinfoErr := os.Stat(src)
				if srcFileinfoErr != nil {
					log.Println(srcFileinfoErr)
					continue
				}

				// Check that file write cooldown period has passed
				if srcFileinfo.ModTime().Add(time.Second * time.Duration(fileWriteCooldownSeconds)).After(time.Now()) {
					log.Printf("write cooldown in effect: %s", src)
					continue
				}

				// Check that src file is not being processed already
				if srcIsMoving(src) {
					log.Printf("file currently moving: %s", src)
					continue
				}

				// Remove .xfer suffix from source filename to create destination filename
				dstFilename := strings.TrimSuffix(srcFilename, transferSuffix)
				dst := fmt.Sprintf("%s/%s", dstPathAbsolute, dstFilename)

				go moveFile(src, dst)
			}
		}

		time.Sleep(time.Second * 5)
	}
}

func moveFile(src, dst string) {
	movingFilesMutex.Lock()
	movingFiles = append(movingFiles, src)
	movingFilesMutex.Unlock()

	log.Printf("moving %s -> %s\n", src, dst)
	moveErr := os.Rename(src, dst)
	if moveErr != nil {
		log.Println(moveErr)
	}
	log.Printf("complete %s -> %s\n", src, dst)

	movingFilesMutex.Lock()
	movingFiles = append(movingFiles, src)
	for i, srcEntry := range movingFiles {
		if srcEntry == src {
			movingFiles = append(movingFiles[:i], movingFiles[i+1:]...)
			break
		}
	}
	movingFilesMutex.Unlock()
}

func main() {

	err := godotenv.Load()
	if err != nil {
		log.Println("Error loading .env file")
	}

	incomingTransferPath := os.Getenv("FARMER_INCOMING_TRANSFER_PATH")
	if len(incomingTransferPath) == 0 {
		incomingTransferPath = "./"
	}

	plotStoragePath := os.Getenv("FARMER_PLOT_STORAGE_PATH")
	if len(plotStoragePath) == 0 {
		plotStoragePath = "./"
	}

	fileWriteCooldownSecondsEnvVar := os.Getenv("FARMER_FILE_WRITE_COOLDOWN_SECONDS")
	if len(fileWriteCooldownSecondsEnvVar) == 0 {
		fileWriteCooldownSeconds = 30
	} else {
		var cooldownParseErr error
		fileWriteCooldownSeconds, cooldownParseErr = strconv.Atoi(os.Getenv("FARMER_FILE_WRITE_COOLDOWN_SECONDS"))
		if cooldownParseErr != nil {
			log.Println("could not parse cooldown seconds, using default")
			fileWriteCooldownSeconds = 30
		}
	}

	transferSuffix = os.Getenv("FARMER_TRANSFER_SUFFIX")
	if len(transferSuffix) == 0 {
		transferSuffix = ".xfer"
	}

	monitorForTransferFiles(incomingTransferPath, plotStoragePath)
}