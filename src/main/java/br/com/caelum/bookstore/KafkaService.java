package br.com.caelum.bookstore;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

public class KafkaService<T> implements Closeable {

  private static final Logger logger = LoggerFactory.getLogger(KafkaService.class);

  private final String consumerGroup;
  private final Class<T> expectedType;
  private final ConsumerFunction<T> recordParser;
  private Map<String, String> extraProperties;
  private final KafkaConsumer<String, T> consumer;

  private KafkaService(String consumerGroup, ConsumerFunction<T> recordParser, Class<T> expectedType) {
    this.consumerGroup = consumerGroup;
    this.expectedType = expectedType;
    this.recordParser = recordParser;
    this.extraProperties = Map.of();
    consumer = new KafkaConsumer<>(properties());
  }

  private KafkaService(String consumerGroup, ConsumerFunction<T> recordParser, Class<T> expectedType, Map<String, String> extraProperties) {
    this.consumerGroup = consumerGroup;
    this.recordParser = recordParser;
    this.expectedType = expectedType;
    this.extraProperties = extraProperties;
    consumer = new KafkaConsumer<>(properties());
  }

  public KafkaService(String consumerGroup, String topic, ConsumerFunction<T> recordParser, Class<T> expectedType) {
    this(consumerGroup, recordParser, expectedType);
    consumer.subscribe(Collections.singletonList(topic));
  }

  public KafkaService(String consumerGroup, String topic, ConsumerFunction<T> recordParser, Class<T> expectedType, Map<String, String> extraProperties) {
    this(consumerGroup, recordParser, expectedType, extraProperties);
    consumer.subscribe(Collections.singletonList(topic));
  }

  public KafkaService(String consumerGroup, Pattern pattern, ConsumerFunction<T> recordParser, Class<T> expectedType) {
    this(consumerGroup, recordParser, expectedType);
    consumer.subscribe(pattern);
  }

  public KafkaService(String consumerGroup, Pattern pattern, ConsumerFunction<T> recordParser, Class<T> expectedType, Map<String, String> extraProperties) {
    this(consumerGroup, recordParser, expectedType, extraProperties);
    consumer.subscribe(pattern);
  }

  public void run() {
    while (true) {
      ConsumerRecords<String, T> records = consumer.poll(Duration.ofMillis(100)); // busy wait
      if (!records.isEmpty()) {
        logger.info("Found " + records.count() + " records.");
        for (ConsumerRecord<String, T> record : records) {
          T value = record.value();

          logger.info("I got a new message: " + value + " . Key: " + record.key()
            + ", Partition: " + record.partition() + ", Offset: " + record.offset());

          recordParser.parse(record);
        }
      }
    }
  }

  private Properties properties() {
    var properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
    properties.setProperty(GsonDeserializer.TYPE_CONFIG, expectedType.getName());
//    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.putAll(extraProperties);
    return properties;
  }

  @Override
  public void close() {
    consumer.close();
  }
}
