package common

import (
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
