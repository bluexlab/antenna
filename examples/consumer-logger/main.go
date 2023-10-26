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
)

func logDebug(msg string, args ...any) {
	slog.Debug(fmt.Sprintf(msg, args...))
}

func logError(msg string, args ...any) {
	slog.Error(fmt.Sprintf(msg, args...))
}

func main() {
	logLevel := &slog.LevelVar{}
	logLevel.Set(slog.LevelDebug)
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: logLevel}))
	slog.SetDefault(logger)

	topic := "test"
	brokers := []string{"localhost:9092"}
	groupID := "test-group"
	worker := antenna.NewAntenna(
		topic,
		brokers,
		groupID,
		antenna.WithLogger(antenna.LoggerFunc(logDebug)),
		antenna.WithErrorLogger(antenna.LoggerFunc(logError)),
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
