package br.com.caelum.bookstore;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

public class LogService {

  public static void main(String[] args) {

    try (var service = new KafkaService(LogService.class.getSimpleName(),
                                        Pattern.compile("^BOOK_.+"),
                                        record -> {
                                          var value = record.value();
                                          System.out.println("Sending to ElasticSearch: " + value);
                                        },
                                        String.class,
                                        Map.of(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                                                StringDeserializer.class.getName()))) {
      service.run();
    }
  }
}
