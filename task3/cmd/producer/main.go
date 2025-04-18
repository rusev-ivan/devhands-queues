package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"math/rand"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

var topic string
var keysString string

func main() {
	flag.StringVar(&topic, "t", "pulse-test", "kafka topic")
	flag.StringVar(&keysString, "k", "aa,bb", "kafka topic")
	flag.Parse()

	kafkaClient, err := kgo.NewClient(
		kgo.SeedBrokers("localhost:19094", "localhost:29094", "localhost:39094"),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		panic(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	slog.Info("Start produce messages")

	for {
		select {
		case <-ctx.Done():
			slog.Info("Stop produce messages")
			return
		default:
			produceMessage(kafkaClient)
			time.Sleep(time.Second)
		}
	}
}

func produceMessage(kafkaClient *kgo.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	keys := strings.Split(keysString, ",")

	var wg sync.WaitGroup
	wg.Add(1)
	kafkaClient.Produce(
		ctx,
		&kgo.Record{
			Key:   []byte(keys[rand.Intn(len(keys))] + strconv.Itoa(1)),
			Value: []byte([]string{"good-", "bad-"}[rand.Intn(2)] + strconv.Itoa(rand.Intn(100)+1)),
			Topic: topic,
		},
		func(record *kgo.Record, err error) {
			defer wg.Done()
			if err != nil {
				slog.Error("error produce message", slog.Any("error", err))
			} else {
				fmt.Println("Sent:",
					slog.Any("key", string(record.Key)),
					slog.Any("value", string(record.Value)),
				)
			}
		},
	)

	wg.Wait()
}
