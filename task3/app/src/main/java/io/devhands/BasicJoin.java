package io.devhands;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;

import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.SlidingWindows;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class BasicJoin {

    public static void main(String[] args) throws IOException {
        Properties props = new Properties();
        try (FileInputStream fis = new FileInputStream("streams.properties")) {
            props.load(fis);
        }

        props.put("application.id", "basic-streams");
        props.put("client.id", "basic-streams-client");
        props.put("consumer.group.instance.id", "consumer-id-1");

        final String sourceTopic = "streams-input";
        final String outputTopic = "streams-joined-output";
        final String joinTopic = "fruits";
        final String searchPrefix = "good-";

        StreamsBuilder builder = new StreamsBuilder();

        System.out.println("Consuming from topic [" + sourceTopic + " + " + joinTopic + "] and producing to [" + outputTopic + "] via " + props.get("bootstrap.servers"));

        GlobalKTable<Long, String> globalTable = builder.globalTable(
            joinTopic,
            Materialized.with(Serdes.Long(), Serdes.String())
        );

        KStream<String, String> sourceStream = builder.stream(sourceTopic,
             Consumed.with(Serdes.String(), Serdes.String()));

        sourceStream
            .peek((key, value) -> System.out.println("In  >> key: " + key + ":\t" + value))

            .filter((key, value) -> value.contains(searchPrefix))
            .mapValues(value -> Long.parseLong(value.substring(value.indexOf("-") + 1)))

            .join(
                globalTable,
                (key, value) -> value,
                // (key, value) -> Long.parseLong(value.substring(value.indexOf("-") + 1)),
                (streamVal, tableVal) -> tableVal
            )

            .peek((key, value) -> System.out.println("Out << key: " + key + ":\t" + value))

            .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()))
        ;

        try (KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), props)) {
            final CountDownLatch shutdownLatch = new CountDownLatch(1);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                kafkaStreams.close(Duration.ofSeconds(1));
                shutdownLatch.countDown();
            }));
            try {
                kafkaStreams.start();
                shutdownLatch.await();
            } catch (Throwable e) {
                System.exit(1);
            }
        }
        System.exit(0);
    }
}