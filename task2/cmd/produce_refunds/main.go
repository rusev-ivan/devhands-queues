package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/rusev-ivan/devhands-queues/task2/internal"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	config, err := internal.NewConfig()
	if err != nil {
		panic(err)
	}

	logger := internal.NewLogger()

	kafkaClient, err := internal.NewProducerKafkaClient(config)
	if err != nil {
		panic(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger.Info("Start produce messages")

	for {
		select {
		case <-ctx.Done():
			logger.Info("Stop produce messages")
			return
		default:
			produceMessage(logger, kafkaClient)
			time.Sleep(time.Second + time.Duration(rand.Int63n(int64(time.Second*5))))
		}
	}
}

func produceMessage(logger *slog.Logger, kafkaClient *kgo.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	operation := internal.Operation{
		TransactionID: uuid.New(),
		Type:          internal.RefundOperationType,
		UserID:        rand.Intn(9999999) + 1,
		Amount:        rand.Intn(99999) + 1,
	}

	log := logger.With(slog.Any("operation", operation))

	operationBytes, _ := json.Marshal(operation)

	var wg sync.WaitGroup
	wg.Add(1)
	kafkaClient.Produce(
		ctx,
		&kgo.Record{
			Value: operationBytes,
			Key:   []byte(strconv.Itoa(operation.UserID)),
			Topic: "payment_operations",
		},
		func(record *kgo.Record, err error) {
			defer wg.Done()
			if err != nil {
				log.Error("error produce message", slog.Any("error", err))
			} else {
				fmt.Println(string(operationBytes))
			}
		},
	)

	wg.Wait()
}
