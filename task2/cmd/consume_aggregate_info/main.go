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

var (
	deposits  Aggregate
	purchases Aggregate
	refunds   Aggregate
)

func main() {
	config, err := internal.NewConfig()
	if err != nil {
		panic(err)
	}

	logger := internal.NewLogger()

	kafkaClient, err := internal.NewConsumerKafkaClient(config, "aggregate-operations")
	if err != nil {
		panic(err)
	}

	kafkaClient.AddConsumeTopics("payment_operations")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	logger.Info("Start consume messages")
	for {
		select {
		case <-ctx.Done():
			logger.Info("Stop consume messages")
			return
		case <-ticker.C:
			printResult()
		default:
			consumeMessages(logger, kafkaClient)
		}
	}
}

func consumeMessages(logger *slog.Logger, kafkaClient *kgo.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	fs := kafkaClient.PollRecords(ctx, 10)

	var wg sync.WaitGroup
	wg.Add(fs.NumRecords())

	fs.EachRecord(func(r *kgo.Record) {
		defer wg.Done()

		var operation internal.Operation
		_ = json.Unmarshal(r.Value, &operation)

		switch operation.Type {
		case internal.DepositOperationType:
			deposits.AddOperation(operation)
		case internal.PurchaseOperationType:
			purchases.AddOperation(operation)
		case internal.RefundOperationType:
			refunds.AddOperation(operation)
		}
	})

	wg.Wait()
	commitOffsets(logger, kafkaClient)
	kafkaClient.AllowRebalance()
}

func printResult() {
	if len(deposits.operations)+len(purchases.operations)+len(refunds.operations) == 0 {
		return
	}

	fmt.Println(fmt.Sprintf("deposits: %s", deposits.Result()))
	fmt.Println(fmt.Sprintf("purchases: %s", purchases.Result()))
	fmt.Println(fmt.Sprintf("refunds: %s", refunds.Result()))
	fmt.Println()
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

type Aggregate struct {
	operations []internal.Operation
	mu         sync.RWMutex
}

func (a *Aggregate) AddOperation(operation internal.Operation) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.operations = append(a.operations, operation)
}

func (a *Aggregate) amountSum() int {
	var result int

	for _, operation := range a.operations {
		result += operation.Amount
	}

	return result
}

func (a *Aggregate) Result() string {
	a.mu.Lock()
	defer a.mu.Unlock()

	result := fmt.Sprintf("%d operations with summary ammount %d", len(a.operations), a.amountSum())
	a.operations = a.operations[:0]

	return result
}
