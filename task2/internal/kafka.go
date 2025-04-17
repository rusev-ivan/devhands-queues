package internal

import (
	"github.com/spf13/viper"
	"github.com/twmb/franz-go/pkg/kgo"
)

func NewProducerKafkaClient(cfg *viper.Viper) (*kgo.Client, error) {
	return kgo.NewClient(
		kgo.SeedBrokers(cfg.GetStringSlice("kafka.brokers")...),
		kgo.AllowAutoTopicCreation(),
	)
}

func NewConsumerKafkaClient(cfg *viper.Viper, consumerGroup string) (*kgo.Client, error) {
	return kgo.NewClient(
		kgo.SeedBrokers(cfg.GetStringSlice("kafka.brokers")...),
		kgo.ConsumerGroup(consumerGroup),
		kgo.DisableAutoCommit(),
		kgo.Balancers(kgo.CooperativeStickyBalancer()),
		kgo.BlockRebalanceOnPoll(),
		kgo.AllowAutoTopicCreation(),
	)
}
