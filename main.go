package main

import (
	"context"
	"fmt"
	"log"
)

var handler Handler = WrapWithSubstringMatcher(
	HandlerFunc(func(ctx context.Context, b Batch) error {
		// No-op or your real processing
		return nil
	}),
	[]string{"ERROR"},
	true, // case-insensitive
	func(ctx context.Context, rec []byte, pattern string) {
		fmt.Printf("MATCH %q in line: %s\n", pattern, rec)
	},
)

func main() {
	path := "tests/big.log"
	fmt.Printf("Starting: %s\n", path)
	if err := ProcessFile(context.Background(), path, handler, Config{}); err != nil {
		log.Fatal(err)
	}
	fmt.Println("Done")
}
