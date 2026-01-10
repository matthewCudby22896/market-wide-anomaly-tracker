package main

import (
	"context"
	"log"
	"os"
	"time"

	massive "github.com/massive-com/client-go/v2/rest"
	"github.com/massive-com/client-go/v2/rest/models"
)

func main() {
	key := os.Getenv("MASSIVE_API_KEY")

	c := massive.New(key)

	from, err := time.Parse("2006-01-02", "2023-01-09")

	if err != nil {
		log.Fatalf("Error parsing 'from' date: %v", err)
	}

	to, err := time.Parse("2006-01-02", "2023-02-10")
	if err != nil {
		log.Fatalf("Error parsing 'to' date: %v", err)
	}

	params := models.ListAggsParams{
		Ticker:     "AAPL",
		Multiplier: 1,
		Timespan:   "second",
		From:       models.Millis(from),
		To:         models.Millis(to),
	}.
		WithAdjusted(true).
		WithOrder(models.Order("asc")).
		WithLimit(120)

	iter := c.ListAggs(context.Background(), params)

	for iter.Next() {
		bar := iter.Item()
		log.Printf("The type is: %T", bar)
		log.Printf("%v", bar)
	}

	if iter.Err() != nil {
		log.Fatal(iter.Err())
	}

}
