package antenna

// Logger interface API for log.Logger.
type Logger interface {
	Printf(string, ...interface{})
}

// LoggerFunc is a bridge between Logger and any third party logger
// Usage:
//
//	l := NewLogger() // some logger
//	a := antenna.NewAntenna(...)
//	a.SetLogger(LoggerFunc(l.Infof))
//	a.SetErrorLogger(LoggerFunc(l.Errorf))
type LoggerFunc func(msg string, args ...any)

// Printf implements Logger interface.
func (f LoggerFunc) Printf(msg string, args ...any) { f(msg, args...) }
