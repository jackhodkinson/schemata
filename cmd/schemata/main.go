package main

import (
	"errors"
	"fmt"
	"os"

	"github.com/jackhodkinson/schemata/internal/cli"
)

func main() {
	if err := cli.Execute(); err != nil {
		if errors.Is(err, cli.ErrDriftDetected) {
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(2)
	}
}
