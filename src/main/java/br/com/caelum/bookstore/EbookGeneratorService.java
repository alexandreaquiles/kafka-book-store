package br.com.caelum.bookstore;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

public class EbookGeneratorService {

  private static final Logger logger = LoggerFactory.getLogger(EbookGeneratorService.class);

  public static void main(String[] args) {
    var consumer = new KafkaConsumer<String, String>(properties());
    consumer.subscribe(Collections.singletonList("BOOK_ORDERS"));

    while (true) {
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100)); // busy wait
      if (!records.isEmpty()) {
        logger.info("Found " + records.count() + " records.");
        for (ConsumerRecord<String, String> record : records) {
          String value = record.value();
          logger.info("I got a new message: " + value + " . Key: " + record.key()
            + ", Partition: " + record.partition() + ", Offset: " + record.offset());

          System.out.println("Generating ebook: " + value);
          try {
            Thread.sleep(5000);
          } catch (InterruptedException e) {
            logger.error("Error while generating ebook", e);
          }
          System.out.println("Ebook generated: " + value);

        }
      }
    }
  }

  private static Properties properties() {
    var properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, EbookGeneratorService.class.getSimpleName());
//    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    return properties;
  }

}
