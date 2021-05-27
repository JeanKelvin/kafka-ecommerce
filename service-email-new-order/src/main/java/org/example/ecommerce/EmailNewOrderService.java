package org.example.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.example.ecommerce.consumer.ConsumerService;
import org.example.ecommerce.consumer.ServiceRunner;
import org.example.ecommerce.dispatcher.KafkaDispatcher;

import java.util.concurrent.ExecutionException;

public class EmailNewOrderService implements ConsumerService<Order> {

    public static void main(String[] args) {
        new ServiceRunner<>(EmailNewOrderService::new).start(1);
    }

    private final KafkaDispatcher<String> emailDispatcher = new KafkaDispatcher<>();

    @Override
    public String getTopic() {
        return "ECOMMERCE_NEW_ORDER";
    }

    @Override
    public String getConsumerGroup() {
        return EmailNewOrderService.class.getSimpleName();
    }

    @Override
    public void parse(ConsumerRecord<String, Message<Order>> record) throws ExecutionException, InterruptedException {
        System.out.println("----------------------------------------------");
        System.out.println("Processing new order, preparing email");
        Message<Order> message = record.value();
        System.out.println(message);

        var order = message.getPayload();
        var emailCode = "Thank you, we are processing your order!";
        CorrelationId id = message.getId().continueWith(EmailNewOrderService.class.getSimpleName());
        emailDispatcher.send("ECOMMERCE_SEND_EMAIL", order.getEmail(), id, emailCode);
    }
}