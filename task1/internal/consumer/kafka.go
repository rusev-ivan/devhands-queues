package consumer

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/rusev-ivan/devhands-queues/task1/internal"
	"github.com/twmb/franz-go/pkg/kgo"
)

type KafkaConsumer struct {
	logger         *slog.Logger
	client         *kgo.Client
	maxPollRecords int
	stopCh         chan struct{}
	wg             sync.WaitGroup
}

func NewKafkaConsumer(
	logger *slog.Logger,
	client *kgo.Client,
) *KafkaConsumer {
	return &KafkaConsumer{
		logger:         logger,
		client:         client,
		maxPollRecords: 100,
		stopCh:         make(chan struct{}),
	}
}

func (c *KafkaConsumer) Run(topic string, handlingDuration time.Duration) {
	c.client.AddConsumeTopics(topic)

	for {
		select {
		case <-c.stopCh:
			c.logger.Info("stopped consume messages")
			return
		default:
			c.consumeRecords(handlingDuration)
		}
	}
}
func (c *KafkaConsumer) consumeRecords(handlingDuration time.Duration) {
	pullCtx, pullCtxCancel := context.WithTimeout(context.Background(), time.Second)
	defer pullCtxCancel()

	fs := c.client.PollRecords(pullCtx, c.maxPollRecords)

	c.wg.Add(len(fs.Records()))

	fs.EachRecord(func(r *kgo.Record) {
		defer c.wg.Done()

		var task internal.Task
		_ = json.Unmarshal(r.Value, &task)

		time.Sleep(handlingDuration)
		c.logger.Info(
			"message consumed",
			slog.String("task", string(r.Value)),
			slog.Int64("offset", r.Offset),
		)
	})

	c.wg.Wait()
	c.CommitOffsets()
	c.client.AllowRebalance()
}

func (c *KafkaConsumer) CommitOffsets() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err := c.client.CommitUncommittedOffsets(ctx)
	if err != nil {
		c.logger.Error(
			"commit message error",
			slog.Any("error", err),
		)
	}
}

func (c *KafkaConsumer) Stop() {
	c.stopCh <- struct{}{}
}
