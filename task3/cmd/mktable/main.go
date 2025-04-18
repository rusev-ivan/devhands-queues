package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

var words = []string{
	"apple", "banana", "cherry", "date", "elderberry", "fig", "grape", "honeydew", "kiwi", "lemon",
	"mango", "nectarine", "orange", "papaya", "quince", "raspberry", "strawberry", "tangerine", "ugli", "vanilla",
	"watermelon", "xigua", "yam", "zucchini", "apricot", "blackberry", "blueberry", "cantaloupe", "cranberry", "dragonfruit",
	"gooseberry", "grapefruit", "guava", "huckleberry", "jackfruit", "kumquat", "lime", "lychee", "mulberry", "olive",
	"passionfruit", "peach", "pear", "persimmon", "pineapple", "plum", "pomegranate", "pomelo", "rambutan", "redcurrant",
	"starfruit", "tamarind", "tomato", "boysenberry", "cloudberry", "durian", "feijoa", "jabuticaba", "jambul", "jostaberry",
	"kiwifruit", "langsat", "longan", "loquat", "madrono", "mangosteen", "marionberry", "medlar", "miracle", "nance",
	"nance", "naranjilla", "pecan", "pequi", "pitanga", "pitaya", "plantain", "pulasan", "purple", "rambai",
	"salak", "santol", "satsuma", "soursop", "surinam", "tangelo", "tayberry", "velvet", "voavanga", "white",
	"yunnan", "zinfandel", "ziziphus", "acerola", "akee", "ackee", "ambarella", "bignay", "bilberry", "biriba",
}

func main() {
	kafkaClient, err := kgo.NewClient(
		kgo.SeedBrokers("localhost:19094", "localhost:29094", "localhost:39094"),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		panic(err)
	}

	for i, word := range words {
		produceMessage(kafkaClient, i, word)
	}
}

func produceMessage(kafkaClient *kgo.Client, key int, value string) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	keyBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(keyBytes, uint64(key))

	var wg sync.WaitGroup
	wg.Add(1)
	kafkaClient.Produce(
		ctx,
		&kgo.Record{
			Key:   keyBytes,
			Value: []byte(value),
			Topic: "fruits",
		},
		func(record *kgo.Record, err error) {
			defer wg.Done()
			if err != nil {
				slog.Error("error produce message", slog.Any("error", err))
			} else {
				fmt.Println("Sent:",
					slog.Any("key", binary.BigEndian.Uint64(keyBytes)),
					slog.Any("value", string(record.Value)),
				)
			}
		},
	)

	wg.Wait()
}
