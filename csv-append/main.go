package main

import (
	"fmt"
	"os"
)

// OpenCsvFileForAppending :
func OpenCsvFileForAppending(outputPath, fileName string) *os.File {
	fullFileName := ""
	if outputPath != "" {
		if err := os.MkdirAll(outputPath, os.ModePerm); err != nil {
			panic(err)
		}
		fullFileName = fmt.Sprintf("%s/%s.csv", outputPath, fileName)
	} else {
		fullFileName = fmt.Sprintf("%s.csv", fileName)
	}

	file, err := os.OpenFile(fullFileName, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		panic(err)
	}
	return file
}
