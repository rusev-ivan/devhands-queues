Поднять сервисы в docker
```shell
make up
```

Остановить и удалить контейнеры
```shell
make down
```

Запустить Kafka producing
```shell
make run-kafka-producer

docker-compose exec app go run main.go kafka producer --topic=queues --period=50ms
```

Запустить Kafka consuming
```shell
make run-kafka-consumer

docker-compose exec app go run main.go kafka consumer --topic=queues --handling-duration=60ms
```

Kafka UI - http://localhost:8080/