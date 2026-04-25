package replayengine

import (
	"log/slog"
	"os"
)

type Logger struct {
	slog.Logger
}

const keyComponent = "component-id"
const keyChild = "child-id"

func NewComponentLogger(componentID string) *Logger {
	componentAttr := slog.String(keyComponent, componentID)

	slogger := slog.New(slog.NewTextHandler(os.Stdout, nil).WithAttrs([]slog.Attr{componentAttr}))

	return &Logger{
		*slogger,
	}
}

// To be called when a component calls Start() on a child component
func (l *Logger) LogStartChild(childID string) {
	l.Debug("starting child", keyChild, childID)
}

// To be called when a component calls Stop() on a child component
func (l *Logger) LogStopChild(childID string) {
	l.Debug("stopping child", keyChild, childID)
}

// To be called to log the shutdown of the loggers component
func (l *Logger) LogShutdown(args ...any) {
	l.Debug("shutdown")
}

// To be called to log the start of the loggers component
func (l *Logger) LogStart(args ...any) {
	l.Debug("shutdown")
}

// Logs an error and then exits with a 1 status code
func (l *Logger) Fatal(msg string, args ...any) {
	l.Error("FATAL - "+msg, args...)
	os.Exit(1)
}

