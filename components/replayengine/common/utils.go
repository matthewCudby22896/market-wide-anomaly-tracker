package common

import (
	"log"
	"time"

	"cloud.google.com/go/civil"
)

func NYSEOpenUnixMilli(day civil.Date) int64 {
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

func NYSECloseUnixMilli(day civil.Date) int64 {
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

func UnixMilliToTimestampNYC(unixMilli int64) string {
	t := time.UnixMilli(unixMilli)
	
	// Load New York location
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		// Fallback to UTC if the timezone DB isn't available
		return t.Format(time.RFC3339)
	}
	
	// Convert to NYC and format using the RFC3339 constant
	return t.In(loc).Format(time.RFC3339)
}

func IsWeekday(date civil.Date) bool {
	t := date.In(time.UTC)
	day := t.Weekday()
	return day >= 1 && day <= 5
}