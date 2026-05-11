package defaults

import "cloud.google.com/go/civil"

const (
	DefaultPostgresDB       = "postgres"
	DefaultPostgresPassword = "password"

	DefaultTimescale float32 = 5.0
)

var DefaultDay = civil.Date{Year: 2025, Month: 3, Day: 20}
