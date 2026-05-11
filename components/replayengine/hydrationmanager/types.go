package hydrationmanager

import (
	"errors"

	"cloud.google.com/go/civil"
)

// Public

type HydrationSuccess struct {
	Symbol string
	Date   civil.Date
}

type HydrationFailure struct {
	Symbol string
	Date   civil.Date
}

var AlreadyHydratedErr error = errors.New("symbol is already hydrated")

var AlreadyHydratingErr error = errors.New("symbol is currently hydrating")

// Private

type hydrationStatus int

const (
	None hydrationStatus = iota // state not previously tracked
	Ready
	Hydrating
	Failed // indicates that hydration was attempted but failed
)

type dataQuery struct {
	Symbol string
	date   civil.Date
}
