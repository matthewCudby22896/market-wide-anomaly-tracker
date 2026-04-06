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
	// UnixMilli returns the local Time corresponding to the given Unix time,
	// msec milliseconds since January 1, 1970 UTC.
	t := time.UnixMilli(unixMilli)
	loc, _ := time.LoadLocation("America/New_York")
	nyTime := t.In(loc)
	timestampStr := nyTime.Format("2006-01-02 03:04:05 PM MST")
	return timestampStr
}
