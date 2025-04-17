package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/rusev-ivan/devhands-queues/task2/internal"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	config, err := internal.NewConfig()
	if err != nil {
		panic(err)
	}

	logger := internal.NewLogger()

	kafkaClient, err := internal.NewConsumerKafkaClient(config, "large-purchase-operations")
	if err != nil {
		panic(err)
	}

	kafkaClient.AddConsumeTopics("payment_operations")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger.Info("Start consume messages")
	for {
		select {
		case <-ctx.Done():
			logger.Info("Stop consume messages")
			return
		default:
			consumeMessages(logger, kafkaClient)
		}
	}
}

func consumeMessages(logger *slog.Logger, kafkaClient *kgo.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	fs := kafkaClient.PollFetches(ctx)

	var wg sync.WaitGroup
	wg.Add(len(fs.Records()))

	fs.EachRecord(func(r *kgo.Record) {
		defer wg.Done()

		var operation internal.Operation
		_ = json.Unmarshal(r.Value, &operation)

		if operation.Type == internal.PurchaseOperationType && operation.Amount > 90_000 {
			fmt.Println(string(r.Value))
		}
	})

	wg.Wait()
	commitOffsets(logger, kafkaClient)
	kafkaClient.AllowRebalance()
}

func commitOffsets(logger *slog.Logger, kafkaClient *kgo.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err := kafkaClient.CommitUncommittedOffsets(ctx)
	if err != nil {
		logger.Error(
			"commit offsets error",
			slog.Any("error", err),
		)
	}
}
