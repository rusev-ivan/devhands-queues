package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"log/slog"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

var topic string
var group string
var stringFormat bool

func main() {
	flag.StringVar(&topic, "t", "pulse-test", "kafka topic")
	flag.StringVar(&group, "g", "test-group", "consumer group")
	flag.BoolVar(&stringFormat, "s", false, "string format for print value")
	flag.Parse()

	kafkaClient, err := kgo.NewClient(
		kgo.SeedBrokers("localhost:19094", "localhost:29094", "localhost:39094"),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.DisableAutoCommit(),
		kgo.Balancers(kgo.CooperativeStickyBalancer()),
		kgo.BlockRebalanceOnPoll(),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		panic(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	slog.Info("Start consume messages")

	for {
		select {
		case <-ctx.Done():
			slog.Info("Stop consume messages")
			return
		default:
			consumeMessages(kafkaClient)
			time.Sleep(time.Second)
		}
	}
}

func consumeMessages(kafkaClient *kgo.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	fs := kafkaClient.PollFetches(ctx)

	var wg sync.WaitGroup
	wg.Add(len(fs.Records()))

	fs.EachRecord(func(r *kgo.Record) {
		defer wg.Done()

		fmt.Println(fmt.Sprintf("[%s/%d]@%d\tKey:%s\t%s", r.Topic, r.Partition, r.Offset, r.Key, parseValue(r.Value)))
	})

	wg.Wait()
	commitOffsets(kafkaClient)
	kafkaClient.AllowRebalance()
}

func commitOffsets(kafkaClient *kgo.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err := kafkaClient.CommitUncommittedOffsets(ctx)
	if err != nil {
		slog.Error(
			"commit offsets error",
			slog.Any("error", err),
		)
	}
}

func parseValue(value []byte) string {
	if stringFormat {
		return string(value)
	}

	var number int64
	err := binary.Read(bytes.NewReader(value), binary.BigEndian, &number)
	if err == nil {
		return strconv.FormatInt(number, 10)
	}

	return string(value)
}
