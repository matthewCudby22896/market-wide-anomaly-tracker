package common

import (
	"log"
	"time"

	"cloud.google.com/go/civil"
)

func GetMarketOpenUnixMilli(day civil.Date) int64 {
	location, err := time.LoadLocation("America/New_York")
	if err != nil {
		log.Fatal(err)
	}
	t := time.Date(
		day.Year,
		day.Month,
		day.Day,
		9, 30, 0, 0, // 9:30:00.000000
		location,
	).UnixMilli()
	return t
}

func GetMarketCloseUnixMilli(day civil.Date) int64 {
	location, err := time.LoadLocation("America/New_York")
	if err != nil {
		log.Fatal(err)
	}
	t := time.Date(
		day.Year,
		day.Month,
		day.Day,
		16, 0, 0, 0,
		location,
	).UnixMilli()
	return t
}
