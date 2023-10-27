package antenna

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/segmentio/kafka-go"
)

// OffsetProvider provides offset for a topic partition
type OffsetProvider interface {
	GetOffset(ctx context.Context, topic string, partition int) (int64, error)
}

// Antenna is a helper for consuming Kafka messages
type Antenna struct {
	topic          string
	brokers        []string
	groupID        string
	offsetProvider OffsetProvider
	handler        Handler
	cg             *kafka.ConsumerGroup
	logger         Logger
	errorLogger    Logger
}

// NewAntenna creates a new Antenna instance
func NewAntenna(
	topic string,
	brokers []string,
	groupID string,
	opts ...Option,
) *Antenna {
	a := &Antenna{
		topic:   topic,
		brokers: brokers,
		groupID: groupID,
	}
	for _, opt := range opts {
		opt(a)
	}
	return a
}

// Run starts the antenna
func (a *Antenna) Run() error {
	config := kafka.ConsumerGroupConfig{
		ID:      a.groupID,
		Brokers: a.brokers,
		Topics:  []string{a.topic},
	}

	group, err := kafka.NewConsumerGroup(config)
	if err != nil {
		return fmt.Errorf("failed to create consumer group: %w", err)
	}
	defer func(c io.Closer) { _ = c.Close() }(group)

	a.cg = group
	a.withLogger(func(logger Logger) {
		logger.Printf("consumer group %s is running", a.groupID)
	})

	var tempDelay time.Duration
	ctx := context.Background()
	for {
		generation, err := group.Next(ctx)
		if errors.Is(err, kafka.ErrGroupClosed) {
			a.withLogger(func(logger Logger) {
				logger.Printf("consumer group %s is closed", a.groupID)
			})
			return nil
		}
		if errors.Is(err, kafka.RebalanceInProgress) {
			a.withLogger(func(logger Logger) {
				logger.Printf("consumer group %s is rebalancing", a.groupID)
			})
			if tempDelay == 0 {
				tempDelay = 500 * time.Millisecond
			} else {
				tempDelay *= 2
			}
			if maxDelay := 10 * time.Second; tempDelay > maxDelay {
				tempDelay = maxDelay
			}

			a.withLogger(func(logger Logger) {
				logger.Printf("retrying in %v", tempDelay)
			})
			time.Sleep(tempDelay)
			continue
		}
		if err != nil {
			return fmt.Errorf("failed to get next generation: %w", err)
		}

		tempDelay = 0

		a.withLogger(func(logger Logger) {
			logger.Printf("consumer group %s is processing generation %d", a.groupID, generation.ID)
		})
		err = a.processGeneration(ctx, generation)
		if err != nil {
			return fmt.Errorf("failed to process generation: %w", err)
		}
	}
}

// Stop stops the antenna
func (a *Antenna) Stop() {
	if a.cg != nil {
		_ = a.cg.Close()
		a.cg = nil
	}
}

func (a *Antenna) processGeneration(ctx context.Context, generation *kafka.Generation) error {
	for topic, assignments := range generation.Assignments {
		for _, assignment := range assignments {
			partition, offset := assignment.ID, assignment.Offset
			a.withLogger(func(logger Logger) {
				logger.Printf("topic %s partition %d offset %d", topic, partition, offset)
			})

			// get last processed offset from offset provider
			if a.offsetProvider != nil {
				lastOffset, err := a.offsetProvider.GetOffset(ctx, topic, partition)
				if err != nil {
					if !errors.Is(err, sql.ErrNoRows) {
						return fmt.Errorf("failed to get offset for topic %s partition %d: %w", topic, partition, err)
					}

					offset = kafka.FirstOffset
					a.withLogger(func(logger Logger) {
						logger.Printf("no offset found for topic %s partition %d, set offset to %d", topic, partition, kafka.FirstOffset)
					})
				} else {
					// the expected offset is the last processed offset + 1
					offset = lastOffset + 1
					a.withLogger(func(logger Logger) {
						logger.Printf("set offset for topic %s partition %d to %d", topic, partition, offset)
					})
				}
			}

			generation.Start(func(ctx context.Context) {
				readerConfig := kafka.ReaderConfig{
					Brokers:   a.brokers,
					Topic:     topic,
					Partition: partition,
				}
				reader := kafka.NewReader(readerConfig)
				defer func(c io.Closer) { _ = c.Close() }(reader)

				if err := reader.SetOffset(offset); err != nil {
					a.withErrorLogger(func(logger Logger) {
						logger.Printf("failed to set offset for topic %s partition %d: %v", topic, partition, err)
					})
					return
				}

				// read messages from kafka
				for {
					msg, err := reader.ReadMessage(ctx)
					if errors.Is(err, kafka.ErrGenerationEnded) {
						a.withLogger(func(logger Logger) {
							logger.Printf("generation ended for topic %s partition %d", topic, partition)
						})
						err = generation.CommitOffsets(map[string]map[int]int64{topic: {partition: offset + 1}})
						if err != nil {
							a.withErrorLogger(func(logger Logger) {
								logger.Printf("failed to commit offset for topic %s partition %d: %v", topic, partition, err)
							})
						}
						return
					}
					if err != nil {
						a.withErrorLogger(func(logger Logger) {
							logger.Printf("failed to read message from topic %s partition %d: %v", topic, partition, err)
						})
						return
					}

					offset = msg.Offset
					a.withLogger(func(logger Logger) {
						logger.Printf("received message from topic %s partition %d offset %d", topic, partition, offset)
					})

					if a.handler == nil {
						continue
					}
					// send kafka message to message handler
					err = a.handler.Process(ctx, msg)
					if err != nil {
						a.withErrorLogger(func(logger Logger) {
							logger.Printf("failed to process message from topic %s partition %d: %v", topic, partition, err)
						})
						return
					}
				}
			})
		}
	}

	return nil
}

func (a *Antenna) withLogger(do func(Logger)) {
	if a.logger != nil {
		do(a.logger)
	}
}

func (a *Antenna) withErrorLogger(do func(Logger)) {
	if a.errorLogger != nil {
		do(a.errorLogger)
	} else {
		a.withLogger(do)
	}
}
