package br.com.caelum.bookstore;

import org.apache.kafka.clients.consumer.ConsumerRecord;

@FunctionalInterface
public interface ConsumerFunction<T> {
  void parse(ConsumerRecord<String, T> record);
}
