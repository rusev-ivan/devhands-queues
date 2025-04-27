package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os/signal"
	"syscall"

	"github.com/nats-io/nats.go"
	"github.com/rusev-ivan/devhands-queues/task5/contract"
	"github.com/rusev-ivan/devhands-queues/task5/orders"
)

var allStatuses = []orders.OrderStatus{
	orders.NewOrderStatus,
	orders.PaidOrderStatus,
	orders.CompletedOrderStatus,
	orders.CanceledOrderStatus,
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	nc, err := nats.Connect("nats://ruser:T0pS3cr3t@localhost:4222")
	if err != nil {
		panic(err)
	}

	subscriber, err := nc.Subscribe("order", func(msg *nats.Msg) {
		var req contract.GetOrderReq
		_ = json.Unmarshal(msg.Data, &req)

		order := orders.Order{
			ID:     req.Id,
			Status: allStatuses[req.Id%len(allStatuses)],
		}

		orderPayload, _ := json.Marshal(order)
		err := nc.Publish(msg.Reply, orderPayload)
		if err != nil {
			fmt.Println("reply error", err)
		} else {
			fmt.Println("replied with message", string(orderPayload))
		}
	})

	select {
	case <-ctx.Done():
		err := subscriber.Unsubscribe()
		if err != nil {
			println("unsubscribe error", err)
		}
	}
}
