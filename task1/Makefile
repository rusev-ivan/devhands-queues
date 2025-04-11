up:
	docker-compose up -d

down:
	docker-compose down

go-tidy:
	docker-compose exec app go mod tidy && go mod vendor

run-kafka-producer:
	docker-compose exec app go run main.go kafka producer --topic=queues --period=50ms

run-kafka-consumer:
	docker-compose exec app go run main.go kafka consumer --topic=queues --handling-duration=60ms