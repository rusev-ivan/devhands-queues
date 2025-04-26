package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgxutil"
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

	producer, err := kgo.NewClient(
		kgo.SeedBrokers(os.Getenv("KAFKA_BROKER")),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		panic(err)
	}

	topic := os.Getenv("KAFKA_TOPIC")

	fmt.Println("Start outbox process")

	for {
		select {
		case <-ctx.Done():
			return
		default:
			processOutboxMessages(ctx, pgConn, producer, topic)
			time.Sleep(time.Second)
		}

	}

}

type OutboxRow struct {
	Id            int
	AccountId     int
	TransactionId int
	Payload       []byte
}

func processOutboxMessages(ctx context.Context, pgConn *pgxpool.Pool, producer *kgo.Client, topic string) {
	tx, err := pgConn.Begin(ctx)
	if err != nil {
		fmt.Println("begin transaction error ", err)
	}

	defer func() {
		if err := tx.Rollback(ctx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
			fmt.Println("rollback transaction error ", err)
		}
	}()

	outboxRows, err := pgxutil.Select(
		ctx,
		tx,
		"SELECT id, account_id, transaction_id, payload FROM outbox WHERE processed = FALSE order by id limit 10 FOR UPDATE",
		nil,
		pgx.RowToStructByName[OutboxRow],
	)

	records := make([]*kgo.Record, len(outboxRows))
	outboxRowIds := make([]int, len(outboxRows))
	for i, outboxRow := range outboxRows {
		fmt.Println(outboxRow.AccountId, outboxRow.TransactionId, string(outboxRow.Payload))

		records[i] = &kgo.Record{
			Topic: topic,
			Key:   []byte(strconv.Itoa(outboxRow.AccountId)),
			Value: outboxRow.Payload,
		}

		outboxRowIds[i] = outboxRow.Id

		if err := randomErr(); err != nil {
			fmt.Println(err)
			return
		}
	}

	produceResult := producer.ProduceSync(ctx, records...)
	if produceResult.FirstErr() != nil {
		fmt.Println("produce batch of records error ", produceResult.FirstErr())
		return
	}

	sql, args := sq.Update("outbox").
		Set("processed", true).
		Where(sq.Eq{"id": outboxRowIds}).
		PlaceholderFormat(sq.Dollar).
		MustSql()
	_, err = tx.Exec(ctx, sql, args...)
	if err != nil {
		fmt.Println("update outbox rows ", err)
	}

	if err := tx.Commit(ctx); err != nil {
		fmt.Println("commit transaction error ", err)
	}
}

func randomErr() error {
	if faulty > rand.Intn(100) {
		return errors.New("Something went wrong... Status unknown.")
	}

	return nil
}
