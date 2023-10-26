package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/bluexlab/antenna"
	"github.com/segmentio/kafka-go"
)

func main() {
	db := newJSONDatabase("db.json")
	messageHandler := antenna.HandlerFunc(func(ctx context.Context, msg kafka.Message) error {
		slog.Info(fmt.Sprintf("Processing Message. Topic: %s, Partition: %d, Offset: %d", msg.Topic, msg.Partition, msg.Offset))
		return db.SaveOffset(ctx, msg.Topic, msg.Partition, msg.Offset)
	})
	logger := antenna.LoggerFunc(func(msg string, args ...any) {
		slog.Info(fmt.Sprintf(msg, args...))
	})

	topic := "test"
	brokers := []string{"localhost:9092"}
	groupID := "test-group"
	worker := antenna.NewAntenna(
		topic,
		brokers,
		groupID,
		antenna.WithOffsetProvider(db),
		antenna.WithMessageHandler(messageHandler),
		antenna.WithLogger(logger),
	)

	// Create context that listens for the interrupt signal from the OS.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var wg sync.WaitGroup

	// Run Antenna
	wg.Add(1)
	go func() {
		defer wg.Done()

		if err := worker.Run(); err != nil {
			slog.Error(err.Error())
			os.Exit(1)
		}
	}()
	slog.Info("Service is running ...")

	// Listen for the interrupt signal.
	<-ctx.Done()
	slog.Info("Shutting down ...")

	// Restore default behavior on the interrupt signal and notify user of shutdown.
	stop()
	slog.Info("Shutting down gracefully, press Ctrl+C again to force")

	worker.Stop()

	wg.Wait()
	slog.Info("Service stopped.")
}
