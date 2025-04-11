package main

import (
	"log"

	"github.com/rusev-ivan/devhands-queues/task1/cmd"
)

func main() {
	err := cmd.Execute()
	if err != nil {
		log.Fatal(err)
	}
}
