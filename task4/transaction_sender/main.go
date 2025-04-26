package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/google/uuid"
)

var transactionId string
var amount int
var userId int
var transactionType = DEPOSIT

type TransactionType string

const (
	DEPOSIT    TransactionType = "DEPOSIT"
	WITHDRAWAL TransactionType = "WITHDRAWAL"
)

func main() {
	flag.StringVar(&transactionId, "id", uuid.New().String(), "id of transaction")
	flag.IntVar(&amount, "amount", 0, "amount of transaction")
	flag.IntVar(&userId, "user-id", 0, "amount of transaction")
	flag.Parse()

	if userId <= 0 || amount == 0 {
		log.Fatal("user-id and amount is required")
	}

	if _, err := uuid.Parse(transactionId); err != nil {
		log.Fatal("transaction id must be uuid")
	}

	if amount < 0 {
		transactionType = WITHDRAWAL
		amount = -amount
	}

	reqBody, err := json.Marshal(map[string]any{
		"accountId":       userId,
		"amount":          amount,
		"transactionType": transactionType,
		"externalId":      transactionId,
	})

	resp, err := http.Post("http://localhost:8000/transactions", "application/json", bytes.NewReader(reqBody))
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}

	if resp.StatusCode == http.StatusOK {
		fmt.Printf("Transaction processed successfully: %s\n", string(respBody))
	} else {
		fmt.Printf("Failed to process transaction: %d %s\n", resp.StatusCode, string(respBody))
	}
}
