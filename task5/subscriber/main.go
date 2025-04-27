package main

import (
	"context"
	"flag"
	"fmt"
	"os/signal"
	"syscall"

	"github.com/nats-io/nats.go"
)

var subject string

func main() {
	flag.StringVar(&subject, "subject", "orders.*", "subject of subscriber")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	nc, err := nats.Connect("nats://ruser:T0pS3cr3t@localhost:4222")
	if err != nil {
		panic(err)
	}

	subscribe, err := nc.Subscribe(subject, func(msg *nats.Msg) {
		fmt.Printf("message recived [%s] %s\n", msg.Subject, msg.Data)
	})

	select {
	case <-ctx.Done():
		err := subscribe.Unsubscribe()
		if err != nil {
			println("unsubscribe error", err)
		}
	}
}
