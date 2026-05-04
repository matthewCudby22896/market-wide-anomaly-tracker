package common

import (
	"errors"
	"log"
	"os"
	"path/filepath"
	"runtime"
)

func GetProjectRoot() string {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		panic("unable to get project root")
	}

	return filepath.Dir(filepath.Dir(filename))
}

func DirExists(path string) bool {
	info, err := os.Stat(path)
	if err == nil {
		return info.IsDir()
	}
	if errors.Is(err, os.ErrNotExist) {
		return false
	}
	log.Fatalf("DirExists() failed unexpectedly: %s", err)
	return false
}

func FileExists(filepath string) bool {
	_, err := os.Stat(filepath)

	if err == nil {
		return true
	}
	if errors.Is(err, os.ErrNotExist) {
		return false
	}
	log.Fatalf("FileExists() failed unexpectedly: %s", err)
	return false
}
