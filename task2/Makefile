up:
	docker-compose up -d

down:
	docker-compose down

go-tidy:
	docker-compose exec app go mod tidy && go mod vendor

produce_deposits:
	docker compose exec app go run cmd/produce_deposits/main.go

produce_purchases:
	docker compose exec app go run cmd/produce_purchases/main.go

produce_refunds:
	docker compose exec app go run cmd/produce_refunds/main.go

consume_aggregate_info:
	docker compose exec app go run cmd/consume_aggregate_info/main.go

consume_large_purchases:
	docker compose exec app go run cmd/consume_large_purchases/main.go