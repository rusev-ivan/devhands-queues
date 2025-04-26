package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgxutil"
	_ "github.com/jackc/pgxutil"
)

var pgConn *pgxpool.Pool
var faulty int

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	var err error

	faulty, err = strconv.Atoi(os.Getenv("FAULTY"))
	if err != nil {
		panic(err)
	}

	pgConn, err = pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		panic(err)
	}

	defer pgConn.Close()

	if err := initDb(ctx, pgConn); err != nil {
		panic(err)
	}

	router := gin.Default()
	router.GET("/", listAccounts)
	router.GET("/balance/:accountId", accountBalance)
	router.POST("/transactions", processTransaction)

	if err := router.Run(":8000"); err != nil {
		panic(err)
	}
}

func listAccounts(c *gin.Context) {
	balances, err := pgxutil.Select(c.Request.Context(), pgConn, "SELECT account_id, current_balance FROM balance order by updated_at desc limit 100", nil, pgx.RowToStructByName[Balance])
	if err != nil {
		internalError(c, fmt.Errorf("get balances: %w", err))
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"balances": balances,
	})
}

func accountBalance(c *gin.Context) {
	accountId := c.Param("accountId")

	currentBalance, err := pgxutil.SelectRow(c.Request.Context(), pgConn, "SELECT current_balance FROM balance WHERE account_id=$1", []any{accountId}, pgx.RowToStructByName[int])
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			c.JSON(http.StatusNotFound, gin.H{
				"error": "Account not found",
			})
			return
		}

		slog.Error("get current balance", slog.Any("error", err))

		internalError(c, fmt.Errorf("get account balance: %w", err))
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"accountId":      accountId,
		"currentBalance": currentBalance,
	})
}

type ProcessTransactionReq struct {
	AccountId       int             `json:"accountId"`
	Amount          int             `json:"amount"`
	TransactionType TransactionType `json:"transactionType"`
	ExternalId      uuid.UUID       `json:"externalId"`
}

func (t ProcessTransactionReq) Valid() bool {
	if t.AccountId <= 0 || t.Amount <= 0 || !t.TransactionType.Valid() || t.ExternalId == uuid.Nil {
		return false
	}
	return true
}

func processTransaction(c *gin.Context) {
	var req ProcessTransactionReq
	err := c.ShouldBindJSON(&req)
	if err != nil || !req.Valid() {
		badRequest(c)
		return
	}

	var balanceDiff = req.Amount
	if req.TransactionType == WITHDRAWAL {
		balanceDiff = -balanceDiff
	}

	ctx := c.Request.Context()

	tx, err := pgConn.Begin(c.Request.Context())
	if err != nil {
		slog.Error("start db transaction", slog.Any("error", err))
		internalError(c, fmt.Errorf("start db transaction: %w", err))
		return
	}

	defer tx.Rollback(ctx)

	var (
		transactionId   int
		balance         int
		amount          int
		transactionType TransactionType
		createdAt       time.Time
	)

	err = tx.QueryRow(
		ctx,
		`
			SELECT 
				t.id, 
				b.current_balance, 
				t.amount,  
				t.transaction_type, 
				t.created_at
			FROM transactions AS t
			JOIN balance AS b ON t.account_id = b.account_id
			WHERE t.external_id = $1 
		 `,
		req.ExternalId,
	).Scan(&transactionId, &balance, &amount, &transactionType, &createdAt)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		internalError(c, fmt.Errorf("find transaction by external_id: %w", err))
		return
	}

	if transactionId != 0 {
		if amount == req.Amount && transactionType == req.TransactionType {
			c.JSON(http.StatusOK, gin.H{
				"status":         "processed",
				"transaction_id": transactionId,
				"external_id":    req.ExternalId,
				"balance":        balance,
				"created_at":     createdAt,
				"duplicate":      true,
			})
			return
		} else {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "Duplicate transaction id with different parameters",
			})
			return
		}
	}

	err = tx.QueryRow(
		ctx,
		`
			INSERT INTO balance (account_id, current_balance, updated_at)
			VALUES ($1, $2, NOW())
			ON CONFLICT (account_id) DO
				UPDATE SET current_balance = balance.current_balance + EXCLUDED.current_balance, updated_at = NOW()
			RETURNING current_balance 
		`,
		req.AccountId, balanceDiff,
	).Scan(&balance)
	if err != nil {
		internalError(c, fmt.Errorf("update balance: %w", err))
		return
	}

	if balance < 0 {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Insufficient balance",
		})
		return
	}

	err = tx.QueryRow(
		ctx,
		`
            INSERT INTO transactions (external_id, account_id, amount, transaction_type, created_at)
            VALUES ($1, $2, $3, $4, NOW())
            RETURNING id, created_at
		`,
		req.ExternalId, req.AccountId, req.Amount, req.TransactionType,
	).Scan(&transactionId, &createdAt)
	if err != nil {
		internalError(c, fmt.Errorf("insert transaction: %w", err))
		return
	}

	outboxPayload, err := json.Marshal(map[string]any{
		"transactionId":   transactionId,
		"externalId":      req.ExternalId,
		"accountId":       req.AccountId,
		"amount":          req.Amount,
		"transactionType": req.TransactionType,
		"timestamp":       createdAt,
	})

	_, err = tx.Exec(
		ctx,
		`
            INSERT INTO outbox (account_id, transaction_id, payload)
            VALUES ($1, $2, $3)
		`,
		req.AccountId, transactionId, outboxPayload,
	)
	if err != nil {
		internalError(c, fmt.Errorf("insert outbox row: %w", err))
		return
	}

	tx.Commit(ctx)

	if err := randomErr(); err != nil {
		internalError(c, err)
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status":         "processed",
		"transaction_id": transactionId,
		"external_id":    req.ExternalId,
		"balance":        balance,
		"created_at":     createdAt,
		"duplicate":      false,
	})
}

func badRequest(c *gin.Context) {
	c.JSON(http.StatusBadRequest, gin.H{
		"error": "Bad request",
	})
}

func internalError(c *gin.Context, err error) {
	c.JSON(http.StatusInternalServerError, gin.H{
		"error": fmt.Sprintf("Internal error: %s", err),
	})
}

type TransactionType string

func (t TransactionType) Valid() bool {
	return slices.Contains([]TransactionType{DEPOSIT, WITHDRAWAL}, t)
}

const (
	DEPOSIT    TransactionType = "DEPOSIT"
	WITHDRAWAL TransactionType = "WITHDRAWAL"
)

type Balance struct {
	AccountId      int
	CurrentBalance int
	UpdatedAt      time.Time
}

func initDb(ctx context.Context, pgConn *pgxpool.Pool) error {
	tx, err := pgConn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}

	defer tx.Rollback(ctx)

	if _, err := pgConn.Exec(ctx, `
	CREATE TABLE IF NOT EXISTS transactions (
        id BIGSERIAL PRIMARY KEY,
        external_id VARCHAR(64) UNIQUE NOT NULL,
        account_id INT NOT NULL,
        amount INT NOT NULL,
        transaction_type VARCHAR(20) NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
	`); err != nil {
		return fmt.Errorf("create transactions table: %w", err)
	}

	if _, err := pgConn.Exec(ctx, `
    CREATE TABLE IF NOT EXISTS balance (
        account_id INT PRIMARY KEY,
        current_balance INT NOT NULL DEFAULT 0.00,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
	`); err != nil {
		return fmt.Errorf("create balance table: %w", err)
	}

	if _, err := pgConn.Exec(ctx, `
    CREATE TABLE IF NOT EXISTS outbox (
        id BIGSERIAL PRIMARY KEY,
        account_id INT NOT NULL,
        transaction_id BIGINT UNIQUE NOT NULL,
        payload JSONB NOT NULL,
        processed BOOLEAN DEFAULT FALSE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
	`); err != nil {
		return fmt.Errorf("create outbox table: %w", err)
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
