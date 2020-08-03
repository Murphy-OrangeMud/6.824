package mr

import (
	"io/ioutil"
	"os"
	"log"
)

func open(filename string) *os.File{
	file, err := os.Open(filename)

	if err != nil {
		log.Fatalf("cannot open %v", filename)
		os.Exit(1)
	}

	return file
}

func readall(filename string) []byte {
	content, err := ioutil.ReadAll(filename)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	return content
}

func readdir(directory string) []os.FileInfo {
	rd, err := ioutil.ReadDir(directory)

	if err != nil {
		log.Fatal("read current directory failed")
		os.Exit(1)
	}

	return rd
}

func exist(filename string) bool {
    _, err := os.Stat(filename)
    return err == nil || os.IsExist(err)
}

func tempfile(dir string, filename string) *os.File {
	tempfile, err := ioutil.TempFile(dir, filename)
				
	if err != nil {
		log.Fatalf("cannot create tempfile %v", filename)
		os.Exit(1)
	}

	return tempfile
}

func create(filename string) *os.File {
	file, err := os.Create(filename)
	
	if err != nil {
		log.Fatalf("cannot create file %v", filename)
		os.Exit(1)
	}

	return file
}