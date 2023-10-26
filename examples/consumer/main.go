package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/bluexlab/antenna"
)

func main() {
	topic := "test"
	brokers := []string{"localhost:9092"}
	groupID := "test-group"
	worker := antenna.NewAntenna(
		topic,
		brokers,
		groupID,
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
