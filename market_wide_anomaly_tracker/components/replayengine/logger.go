package replayengine

import (
	"fmt"
)

type ComponentLogger interface {
	Info(fstring string, args ...any)
	Errorf(fstring string, args ...any)
	LogStartChild(childName string)
	LogShutdownChild(childName string)
	LogShutdown()
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
	fmt.Printf("[%s] INFO - %s\n", l.componentName, fmt.Sprintf(fstring, args...))
}

func (l *logger) Errorf(fstring string, args ...any) {
	fmt.Printf("[%s] ERROR - %s\n", l.componentName, fmt.Sprintf(fstring, args...))
}

func (l *logger) LogStartChild(childName string) {
	l.Info("Starting child component: %s", childName)
}

func (l *logger) LogShutdownChild(childName string) {
	l.Info("Triggering shutdown for child component: %s", childName)
}

func (l *logger) LogShutdown() {
	l.Info("Shutdown complete.")
}