package antenna

import (
	"context"

	"github.com/segmentio/kafka-go"
)

// A Handler responds to a Kafka message
type Handler interface {
	Process(ctx context.Context, msg kafka.Message) error
}

// The HandlerFunc type is an adapter to allow the use of
// ordinary functions as a Kafka handler. If f is a function
// with the appropriate signature, HandlerFunc(f) is a
// Handler that calls f.
type HandlerFunc func(ctx context.Context, msg kafka.Message) error

// Process calls f(ctx, msg).
func (f HandlerFunc) Process(ctx context.Context, msg kafka.Message) error {
	return f(ctx, msg)
}
