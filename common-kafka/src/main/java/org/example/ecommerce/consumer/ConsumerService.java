package org.example.ecommerce.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.example.ecommerce.Message;


public interface ConsumerService<T> {
    void parse(ConsumerRecord<String, Message<T>> record) throws Exception;
    String getTopic();
    String getConsumerGroup();
}
