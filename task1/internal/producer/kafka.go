package producer

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rusev-ivan/devhands-queues/task1/internal"
	"github.com/twmb/franz-go/pkg/kgo"
)

type KafkaProducer struct {
	logger *slog.Logger
	client *kgo.Client
	stopCh chan struct{}
	wg     sync.WaitGroup
}

func NewKafkaProducer(
	logger *slog.Logger,
	client *kgo.Client,
) *KafkaProducer {
	return &KafkaProducer{
		logger: logger,
		client: client,
		stopCh: make(chan struct{}),
	}
}

func (p *KafkaProducer) Run(topic string, period time.Duration) {
	log := p.logger.With(slog.String("topic", topic))

	ticker := time.NewTicker(period)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p.produceMessage(log, topic)
		case <-p.stopCh:
			p.logger.Info("stopped produce messages")
			return
		}
	}
}

func (p *KafkaProducer) produceMessage(log *slog.Logger, topic string) {
	task := internal.Task{ID: uuid.New()}
	taskPayload, _ := json.Marshal(task)
	log = p.logger.With(slog.String("task", string(taskPayload)))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	p.wg.Add(1)
	p.client.Produce(ctx, &kgo.Record{
		Topic: topic,
		Value: taskPayload,
		Key:   []byte(task.ID.String()),
	}, func(record *kgo.Record, err error) {
		defer p.wg.Done()

		if err != nil {
			log.Error(
				"produce message error",
				slog.Any("error", err),
			)
		} else {
			log.Info(
				"message produced",
				slog.Int64("offset", record.Offset),
			)
		}
	})

	p.wg.Wait()
}

func (p *KafkaProducer) Stop() {
	p.stopCh <- struct{}{}
}
