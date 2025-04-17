Запустить докер контейнеры
```shell
make up
```
Create topic
```shell
docker exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh \
 --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 \
 --create \
 --topic test_topic
```

Describe topic
```shell
docker exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh \
 --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 \
 --describe \
 --topic test_topic
```

Run console producer
```shell
docker exec -it kafka-1 /opt/kafka/bin/kafka-console-producer.sh \
 --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 \
 --topic test_topic
```

Run console consumer
```shell
docker exec -it kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
 --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 \
 --from-beginning \
 --group console \
 --topic test_topic
```

# Денежные операции
## Продюсеры
* Продюсер пополнений средств type=deposit 
```shell
make produce_deposits
```
* Продюсер покупок type=purchase
```shell
make produce_purchases
```
* Продюсер возвратов средств type=refund
```shell
make produce_refunds
```

## Консьюмеры
* Консьюмер агрегаций сум для всех типов операций в 10 секунд
```shell
make consume_aggregate_info
```
* Консьюмер выявления покупок с крупной суммой > 90_000
```shell
make consume_large_purchases
```

### Команды остановки и запуска брокеров кафки
```shell
docker compose stop kafka-1
docker compose start kafka-1

docker compose stop kafka-2
docker compose start kafka-2

docker compose stop kafka-3
docker compose start kafka-3
```