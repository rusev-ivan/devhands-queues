package internal

import (
	"github.com/spf13/viper"
	"github.com/twmb/franz-go/pkg/kgo"
)

func NewKafkaClient(cfg *viper.Viper) (*kgo.Client, error) {
	return kgo.NewClient(
		kgo.SeedBrokers(cfg.GetStringSlice("kafka.brokers")...),
		kgo.ConsumerGroup(cfg.GetString("kafka.consumer_group")),
		kgo.DisableAutoCommit(),
		kgo.Balancers(kgo.CooperativeStickyBalancer()),
		kgo.BlockRebalanceOnPoll(),
		kgo.AllowAutoTopicCreation(),
	)
}
