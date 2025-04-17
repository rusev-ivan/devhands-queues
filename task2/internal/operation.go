package internal

import (
	"github.com/google/uuid"
)

type OperationType string

const (
	DepositOperationType  = OperationType("deposit")
	PurchaseOperationType = OperationType("purchase")
	RefundOperationType   = OperationType("refund")
)

type Operation struct {
	TransactionID uuid.UUID     `json:"transactionId"`
	Type          OperationType `json:"type"`
	UserID        int           `json:"userId"`
	Amount        int           `json:"amount"`
}
