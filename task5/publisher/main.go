package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/rusev-ivan/devhands-queues/task5/orders"
)

var allStatuses = []orders.OrderStatus{
	orders.NewOrderStatus,
	orders.PaidOrderStatus,
	orders.CompletedOrderStatus,
	orders.CanceledOrderStatus,
}

func randOrderStatus() orders.OrderStatus {
	return allStatuses[rand.Intn(len(allStatuses))]
}

func genOrder() orders.Order {
	return orders.Order{
		ID:     rand.Intn(100000),
		Status: randOrderStatus(),
	}
}

func main() {
	nc, err := nats.Connect("nats://ruser:T0pS3cr3t@localhost:4222")
	if err != nil {
		panic(err)
	}

	for {
		order := genOrder()
		orderPayload, _ := json.Marshal(order)
		subject := fmt.Sprintf("orders.%s", order.Status)

		err := nc.Publish(subject, orderPayload)
		if err != nil {
			fmt.Println("publish message error", err)
		} else {
			fmt.Printf("message published [%s] %s\n", subject, orderPayload)
		}

		time.Sleep(time.Second)
	}
}
