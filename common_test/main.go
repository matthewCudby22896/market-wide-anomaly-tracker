package main

import (
	"fmt"

	"github.com/mcudby/mwat/common"
)

func main() {
	root := common.GetProjectRoot()

	fmt.Printf("root: %s\n", root)
}
