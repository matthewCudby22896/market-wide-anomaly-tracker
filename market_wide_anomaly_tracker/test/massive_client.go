package main

import (
	"fmt"
	"context"
	"os"
	"log"

	"github.com/massive-com/client-go/v3/rest"
	"github.com/massive-com/client-go/v3/rest/gen"
)

func main() {
	api_key := os.Getenv("MASSIVE_API_KEY")

	c := rest.NewWithOptions(api_key,
		rest.WithTrace(false),
		rest.WithPagination(true),
	)
	ctx := context.Background()

	params := &gen.GetStocksAggregatesParams{
		Adjusted: rest.Ptr(true),
		Sort:     "asc",
		Limit:    rest.Ptr(120),
	}

	resp, err := c.GetStocksAggregatesWithResponse(
		ctx,
		"AAPL",
		5, gen.Second,
		"2026-02-16",
		"2026-02-17",
		params,
	)
	if err != nil {
		log.Fatal(err)
	}

	if err := rest.CheckResponse(resp); err != nil {
		log.Fatal(err)
	}

	iter := rest.NewIteratorFromResponse(c, resp)
	for iter.Next() {
		item := iter.Item()
		fmt.Printf("%+v\n", item)
	}
	if err := iter.Err(); err != nil {
		log.Fatal(err)
	}
}
