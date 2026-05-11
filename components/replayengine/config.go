package replayengine

import (
	"fmt"
	"os"

	"github.com/mcudby/mwat/components/replayengine/defaults"
)

func fmtDBUrl(url string) string {
	database := os.Getenv("POSTGRES_DB")
	if database == "" {
		database = defaults.DefaultPostgresDB
	}
	password := os.Getenv("POSTGRES_PASSWORD")
	if password == "" {
		password = defaults.DefaultPostgresPassword
	}

	return fmt.Sprintf("postgres://%s:%s@%s/postgres?sslmode=disable", database, password, url)
}

const (
	MAX_TIMESCALE float32 = 3600.0
	MIN_TIMESCALE float32 = 0.1
)
