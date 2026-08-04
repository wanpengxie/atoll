package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/wanpengxie/atoll/app/contract"
)

func main() {
	out := flag.String("out", "", "output file")
	flag.Parse()
	if *out == "" {
		fmt.Fprintln(os.Stderr, "-out is required")
		os.Exit(2)
	}
	b, err := contract.GenerateSchema()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	b = append(b, '\n')
	if err := os.WriteFile(*out, b, 0o644); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
