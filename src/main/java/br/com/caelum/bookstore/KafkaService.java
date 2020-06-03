package br.com.caelum.bookstore;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.regex.Pattern;

public class KafkaService implements Closeable {

  private static final Logger logger = LoggerFactory.getLogger(KafkaService.class);

  private final KafkaConsumer<String, String> consumer;
  private final Consumer<ConsumerRecord<String, String>> recordParser;

  private KafkaService(String consumerGroup, Consumer<ConsumerRecord<String, String>> recordParser) {
    this.recordParser = recordParser;
    consumer = new KafkaConsumer<>(properties(consumerGroup));
  }

  public KafkaService(String consumerGroup, String topic, Consumer<ConsumerRecord<String, String>> recordParser) {
    this(consumerGroup, recordParser);
    consumer.subscribe(Collections.singletonList(topic));
  }

  public KafkaService(String consumerGroup, Pattern pattern, Consumer<ConsumerRecord<String, String>> recordParser) {
    this(consumerGroup, recordParser);
    consumer.subscribe(pattern);
  }

  public void run() {
    while (true) {
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100)); // busy wait
      if (!records.isEmpty()) {
        logger.info("Found " + records.count() + " records.");
        for (ConsumerRecord<String, String> record : records) {
          String value = record.value();

          logger.info("I got a new message: " + value + " . Key: " + record.key()
            + ", Partition: " + record.partition() + ", Offset: " + record.offset());

          recordParser.accept(record);
        }
      }
    }
  }

  private static Properties properties(String consumerGroup) {
    var properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
//    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    return properties;
  }

  @Override
  public void close() {
    consumer.close();
  }
}
