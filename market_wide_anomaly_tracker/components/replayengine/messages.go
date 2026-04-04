package replayengine

import "cloud.google.com/go/civil"

type symbolHydrated struct {
	ticker Symbol
	date   civil.Date
}
