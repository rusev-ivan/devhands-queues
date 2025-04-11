Start docker containers
```shell
make up
```

Down docker containers
```shell
make down
```

Start Kafka producing
```shell
make run-kafka-producer

docker-compose exec app go run main.go kafka producer --topic=queues --period=50ms
```

Start Kafka consuming
```shell
make run-kafka-consumer

docker-compose exec app go run main.go kafka consumer --topic=queues --handling-duration=60ms
```

Kafka UI - http://localhost:8080/