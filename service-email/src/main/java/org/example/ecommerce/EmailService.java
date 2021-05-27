package org.example.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.example.ecommerce.consumer.ConsumerService;
import org.example.ecommerce.consumer.ServiceRunner;


public class EmailService implements ConsumerService<String> {

    public static void main(String[] args) {
        new ServiceRunner(EmailService::new).start(5);
    }

    @Override
    public String getTopic() {
        return "ECOMMERCE_SEND_EMAIL";
    }

    @Override
    public String getConsumerGroup() {
        return EmailService.class.getSimpleName();
    }

    @Override
    public void parse(ConsumerRecord<String, Message<String>> record) {
        System.out.println("----------------------------------------------");
        System.out.println("Send email");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("Email sent");
    }
}