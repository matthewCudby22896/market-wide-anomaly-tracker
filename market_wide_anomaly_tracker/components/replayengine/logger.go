package replayengine

import (
	"fmt"
)

type ComponentLogger interface {
	Info(fstring string, args ...any)
}

type logger struct {	
	componentName string
}

func NewLogger(componentName string) *logger {
	return &logger{
		componentName: componentName,	
	}
}

func (l *logger) Info(fstring string, args ...any) {
	fmt.Printf("[%s] %s\n", l.componentName, fmt.Sprintf(fstring, args...))
}