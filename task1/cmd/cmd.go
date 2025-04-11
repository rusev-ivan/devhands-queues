package cmd

import (
	"time"

	"github.com/rusev-ivan/devhands-queues/task1/internal"
	"github.com/rusev-ivan/devhands-queues/task1/internal/consumer"
	"github.com/rusev-ivan/devhands-queues/task1/internal/producer"
	"github.com/spf13/cobra"
	"go.uber.org/fx"
)

var (
	kafkaProducerTopic            string
	kafkaProducerPeriod           time.Duration
	kafkaConsumerTopic            string
	kafkaConsumerHandlingDuration time.Duration
)

func Execute() error {
	fxOptions := []fx.Option{
		fx.NopLogger,
		fx.Provide(
			internal.NewConfig,
			internal.NewLogger,
			internal.NewKafkaClient,
		),
	}

	rootCmd := &cobra.Command{
		Use: "queues",
	}

	kafkaCmd := &cobra.Command{
		Use:   "kafka",
		Short: "Run commands for kafka",
	}

	kafkaProducerCmd := &cobra.Command{
		Use:   "producer",
		Short: "Run kafka producer",
		Run: func(cmd *cobra.Command, args []string) {
			fxOptions = append(fxOptions,
				fx.Provide(
					fx.Annotate(
						producer.NewKafkaProducer,
						fx.OnStart(func(kafkaProducer *producer.KafkaProducer) {
							go kafkaProducer.Run(kafkaProducerTopic, kafkaProducerPeriod)
						}),
						fx.OnStop(func(kafkaProducer *producer.KafkaProducer) {
							kafkaProducer.Stop()
						}),
					),
				),
				fx.Invoke(
					func(kafkaProducer *producer.KafkaProducer) {},
				),
			)

			fx.New(fxOptions...).Run()
		},
	}
	kafkaProducerCmd.PersistentFlags().StringVar(&kafkaProducerTopic, "topic", "queues", "")
	kafkaProducerCmd.PersistentFlags().DurationVar(&kafkaProducerPeriod, "period", time.Second, "duration between publishing messages")

	kafkaConsumerCmd := &cobra.Command{
		Use:   "consumer",
		Short: "Run kafka consumer",
		Run: func(cmd *cobra.Command, args []string) {
			fxOptions = append(fxOptions,
				fx.Provide(
					fx.Annotate(
						consumer.NewKafkaConsumer,
						fx.OnStart(func(kafkaConsumer *consumer.KafkaConsumer) {
							go kafkaConsumer.Run(kafkaConsumerTopic, kafkaConsumerHandlingDuration)
						}),
						fx.OnStop(func(kafkaConsumer *consumer.KafkaConsumer) {
							kafkaConsumer.Stop()
						}),
					),
				),
				fx.Invoke(
					func(kafkaConsumer *consumer.KafkaConsumer) {},
				),
			)

			fx.New(fxOptions...).Run()
		},
	}
	kafkaConsumerCmd.PersistentFlags().StringVar(&kafkaConsumerTopic, "topic", "queues", "")
	kafkaConsumerCmd.PersistentFlags().DurationVar(&kafkaConsumerHandlingDuration, "handling-duration", time.Second, "message handling duration")

	kafkaCmd.AddCommand(kafkaProducerCmd)
	kafkaCmd.AddCommand(kafkaConsumerCmd)
	rootCmd.AddCommand(kafkaCmd)

	return rootCmd.Execute()
}
