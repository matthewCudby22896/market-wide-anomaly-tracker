package replayengine

import (
	"cloud.google.com/go/civil"
	"github.com/mcudby/mwat/components/replayengine/common"
)

type symbolHydrated struct {
	symbol string
	date   civil.Date
}
