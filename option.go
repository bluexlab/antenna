package antenna

// Option is a function that configures an Antenna.
type Option func(*Antenna)

// WithOffsetProvider sets the offset provider for the antenna.
func WithOffsetProvider(offsetProvider OffsetProvider) Option {
	return func(a *Antenna) {
		a.offsetProvider = offsetProvider
	}
}

// WithMessageHandler sets the message handler for the antenna.
func WithMessageHandler(handler Handler) Option {
	return func(a *Antenna) {
		a.handler = handler
	}
}

// WithLogger sets the logger for the antenna.
func WithLogger(logger Logger) Option {
	return func(a *Antenna) {
		a.logger = logger
	}
}

// WithErrorLogger sets the error logger for the antenna.
func WithErrorLogger(logger Logger) Option {
	return func(a *Antenna) {
		a.errorLogger = logger
	}
}
