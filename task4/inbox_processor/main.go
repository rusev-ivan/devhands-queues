package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
)

var faulty int

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	var err error

	faulty, err = strconv.Atoi(os.Getenv("FAULTY"))
	if err != nil {
		panic(err)
	}

	pgConn, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		panic(err)
	}

	defer pgConn.Close()

	err = initDb(ctx, pgConn)
	if err != nil {
		panic(err)
	}

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(os.Getenv("KAFKA_BROKER")),
		kgo.ConsumerGroup(os.Getenv("KAFKA_CONSUMER_GROUP")),
		kgo.ConsumeTopics(os.Getenv("KAFKA_TOPIC")),
		kgo.DisableAutoCommit(),
		kgo.Balancers(kgo.CooperativeStickyBalancer()),
		kgo.BlockRebalanceOnPoll(),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		panic(err)
	}

	fmt.Println("Start inbox process")

	for {
		select {
		case <-ctx.Done():
			return
		default:
			consumeMessages(ctx, consumer, pgConn)
		}
	}
}

func consumeMessages(ctx context.Context, consumer *kgo.Client, pgConn *pgxpool.Pool) {
	consumer.AllowRebalance()
	fs := consumer.PollRecords(nil, 10)
	for _, record := range fs.Records() {
		for {
			err := processMessage(ctx, pgConn, record)
			if err != nil {
				fmt.Println("process message error: ", err)
				continue
			}

			fmt.Println("message processed")
			break
		}
	}

	err := consumer.CommitRecords(ctx, fs.Records()...)
	if err != nil {
		fmt.Println("commit records error: ", err)
	}
}

type TransactionType string

const (
	DEPOSIT    TransactionType = "DEPOSIT"
	WITHDRAWAL TransactionType = "WITHDRAWAL"
)

type TransactionPayload struct {
	TransactionId   int             `json:"transactionId"`
	ExternalId      string          `json:"externalId"`
	AccountId       int             `json:"accountId"`
	Amount          int             `json:"amount"`
	TransactionType TransactionType `json:"transactionType"`
	Timestamp       time.Time       `json:"timestamp"`
}

func processMessage(ctx context.Context, pgConn *pgxpool.Pool, record *kgo.Record) error {
	tx, err := pgConn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("start transaction: %w", err)
	}

	defer tx.Rollback(ctx)

	var transactionPayload TransactionPayload
	err = json.Unmarshal(record.Value, &transactionPayload)
	if err != nil {
		return fmt.Errorf("unmarshal message payload: %w", err)
	}

	if err := randomErr(); err != nil {
		return err
	}

	var inboxRowExist bool
	err = tx.QueryRow(
		ctx,
		"SELECT EXISTS (SELECT 1 FROM inbox WHERE transaction_id = $1)",
		transactionPayload.TransactionId,
	).Scan(&inboxRowExist)
	if err != nil {
		return fmt.Errorf("check exist inbox row: %w", err)
	}

	if inboxRowExist {
		fmt.Printf("Transaction %d already processed. Commit offset %d \n", transactionPayload.TransactionId, record.Offset)
		return nil
	}

	balanceDiff := transactionPayload.Amount
	if transactionPayload.TransactionType == WITHDRAWAL {
		balanceDiff = -balanceDiff
	}

	_, err = tx.Exec(
		ctx,
		`
			INSERT INTO balance (account_id, current_balance, updated_at)
			VALUES ($1, $2, NOW())
			ON CONFLICT (account_id) DO UPDATE SET current_balance = balance.current_balance + EXCLUDED.current_balance, updated_at = NOW();
		`,
		transactionPayload.AccountId, balanceDiff,
	)
	if err != nil {
		return fmt.Errorf("update balance: %w", err)
	}

	if err := randomErr(); err != nil {
		return err
	}

	_, err = tx.Exec(
		ctx,
		`INSERT INTO inbox (transaction_id) VALUES ($1)`,
		transactionPayload.TransactionId,
	)
	if err != nil {
		return fmt.Errorf("insert inbox row: %w", err)
	}

	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("commit db transaction: %w", err)
	}

	if err := randomErr(); err != nil {
		return err
	}

	return nil
}

func initDb(ctx context.Context, pgConn *pgxpool.Pool) error {
	tx, err := pgConn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}

	defer tx.Rollback(ctx)

	if _, err := pgConn.Exec(ctx, `
    CREATE TABLE IF NOT EXISTS inbox (
        transaction_id BIGINT PRIMARY KEY,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS balance (
        account_id INT PRIMARY KEY,
        current_balance INT NOT NULL DEFAULT 0,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
	`); err != nil {
		return fmt.Errorf("create tables: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}

func randomErr() error {
	if faulty > rand.Intn(100) {
		return errors.New("Something went wrong... Status unknown.")
	}

	return nil
}
