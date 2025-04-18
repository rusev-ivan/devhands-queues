Запустить докер контейнеры
```shell
docker compose up -d
```

Run produce messages to streams-input topic
```shell
go run cmd/producer/main.go -t streams-input
```
Run consume messages from topic
```shell
go run cmd/consumer/main.go -t streams-joined-output -s
```
Run produce messages for mktable
```shell
go run cmd/mktable/main.go
```
Run Kafka Stream
```shell
./gradlew run
```