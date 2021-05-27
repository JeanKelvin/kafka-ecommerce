package org.example.ecommerce.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.example.ecommerce.Message;
import org.example.ecommerce.dispatcher.GsonSerializer;
import org.example.ecommerce.dispatcher.KafkaDispatcher;

import java.io.Closeable;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

public class KafkaService<T> implements Closeable {
    private final KafkaConsumer<String, Message<T>> consumer;
    private final ConsumerFunction parse;

    public KafkaService(String groupId, String topic, ConsumerFunction<T> parse, Map<String, String> properties) {
        this(parse, groupId, properties);
        consumer.subscribe(Collections.singleton(topic));
    }

    public KafkaService(String groupId, Pattern topic, ConsumerFunction<T> parse, Map<String, String> properties) {
        this(parse, groupId, properties);
        consumer.subscribe(topic);
    }

    private KafkaService(ConsumerFunction<T> parse, String groupId, Map<String, String> properties) {
        this.parse = parse;
        this.consumer = new KafkaConsumer<>(getProperties(groupId, properties));
    }

    public void run() throws ExecutionException, InterruptedException {
        try(var deadLetter = new KafkaDispatcher<>()) {
            while (true) {
                var records = consumer.poll(Duration.ofMillis(100));

                if (!records.isEmpty()) {
                    System.out.println("Encontrei " + records.count() + " registros");

                    for (var record : records) {
                        try {
                            parse.consume(record);
                        } catch (Exception e) {
                            e.printStackTrace();
                            var message = record.value();
                            deadLetter.send(
                                    "ECOMMERCE_DEADLETTER",
                                    message.getId().toString(),
                                    message.getId().continueWith("DeadLetter"),
                                    new GsonSerializer().serialize("", message));
                        }
                    }
                }
            }
        }
    }

    private Map<String, Object> getProperties(String groupId,  Map<String, String> overrideProperties) {
        HashMap<String, Object> properties = new HashMap<>();
        var bootstrapServers = "127.0.0.1:9091,127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094,127.0.0.1:9095";

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Arrays.asList(bootstrapServers.split(",")));
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.putAll(overrideProperties);

        return properties;
    }

    @Override
    public void close() {
        this.consumer.close();
    }
}
