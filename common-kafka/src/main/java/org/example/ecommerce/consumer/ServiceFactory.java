package org.example.ecommerce.consumer;

@FunctionalInterface
public interface ServiceFactory<T> {

    ConsumerService<T> create() throws Exception;
}
