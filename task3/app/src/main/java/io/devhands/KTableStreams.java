package io.devhands;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class KTableStreams {

    public static void main(String[] args) throws IOException {

        final Properties props = new Properties();
        try (FileInputStream fis = new FileInputStream("streams.properties")) {
            props.load(fis);
        }
        // props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ktable-example");
        props.put("application.id", "ktable-example"); // consumer group

        // props.put(StreamsConfig.consumerPrefix("group.instance.id"), "consumer-id-1");
        props.put("consumer.group.instance.id", "consumer-id-1");

        StreamsBuilder builder = new StreamsBuilder();
        final String inputTopic = props.getProperty("ktable.input.topic");
        final String outputTopic = props.getProperty("ktable.output.topic");

        final String filterPrefix = "good-";

        // Crate a table with the StreamBuilder from above and use the table method
        // along with the inputTopic create a Materialized instance and name the store
        // and provide a Serdes for the key and the value  HINT: Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as
        // then use two methods to specify the key and value serde

        System.out.println("Consuming '" + filterPrefix + "*' from topic [" + inputTopic + "] and producing to [" + outputTopic + "] via " + props.get("bootstrap.servers"));

        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 5000);

        Materialized<String, String, KeyValueStore<Bytes, byte[]>> materialized =
            Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("ktable-store")
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.String())

                .withCachingDisabled()
//                 .withCachingEnabled() // controlled by the commit interval
            ;

        KTable<String, String> table = builder.table(inputTopic, materialized);

        table
            .filter((key, value) -> value.contains(filterPrefix))
            .mapValues(value -> value.substring(value.indexOf("-") + 1))

            .toStream()
            .peek((key, value) -> System.out.println("Out << key: " + key + ":\t" + value))
            .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()))
        ;

        try (KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), props)) {
            final CountDownLatch shutdownLatch = new CountDownLatch(1);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                kafkaStreams.close(Duration.ofSeconds(2));
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
