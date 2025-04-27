package main

import (
	"context"
	"fmt"
	"os/signal"
	"syscall"

	"github.com/nats-io/nats.go"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	nc, err := nats.Connect("nats://ruser:T0pS3cr3t@localhost:4222")
	if err != nil {
		panic(err)
	}

	queueSubscriber, err := nc.QueueSubscribe("orders.*", "orders.processor", func(msg *nats.Msg) {
		fmt.Printf("message recived [%s] %s\n", msg.Subject, msg.Data)
	})

	select {
	case <-ctx.Done():
		err := queueSubscriber.Unsubscribe()
		if err != nil {
			println("unsubscribe error", err)
		}
	}
}
