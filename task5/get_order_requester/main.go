package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/rusev-ivan/devhands-queues/task5/contract"
)

var orderId int

func main() {
	flag.IntVar(&orderId, "order-id", 0, "order id")
	flag.Parse()

	if orderId == 0 {
		panic("order id is required")
	}

	nc, err := nats.Connect("nats://ruser:T0pS3cr3t@localhost:4222")
	if err != nil {
		panic(err)
	}

	reqPayload, _ := json.Marshal(contract.GetOrderReq{Id: orderId})

	response, err := nc.Request("order", reqPayload, 2*time.Second)
	if err != nil {
		fmt.Println("request error", err)
		return
	}

	fmt.Println("Resp:", string(response.Data))
}
