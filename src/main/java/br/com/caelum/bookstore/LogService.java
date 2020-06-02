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
import java.util.Properties;
import java.util.regex.Pattern;

public class LogService {

  private static final Logger logger = LoggerFactory.getLogger(LogService.class);

  public static void main(String[] args) {
    var consumer = new KafkaConsumer<String, String>(properties());
    var regex = Pattern.compile("^BOOK_.+");
    consumer.subscribe(regex);

    while (true) {
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100)); // busy wait
      if (!records.isEmpty()) {
        logger.info("Found " + records.count() + " records.");
        for (ConsumerRecord<String, String> record : records) {
          String value = record.value();

          logger.info("I got a new message: " + value + " . Key: " + record.key()
            + ", Partition: " + record.partition() + ", Offset: " + record.offset());

          System.out.println("Sending to ElasticSearch: " + value);
        }
      }
    }
  }

  private static Properties properties() {
    var properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, LogService.class.getSimpleName());
//    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    return properties;
  }

}
