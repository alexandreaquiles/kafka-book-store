package br.com.caelum.bookstore;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaDispatcher implements Closeable {

  private static final Logger logger = LoggerFactory.getLogger(KafkaDispatcher.class);

  private final KafkaProducer<String, String> producer;

  public KafkaDispatcher( ) {
    producer = new KafkaProducer<>(properties());
  }

  public void send(String topic, String key, String value) throws ExecutionException, InterruptedException {
    Callback callback = (RecordMetadata metadata, Exception exception) -> {
      if (exception != null) {
        logger.error("ERROR when sending record to " + metadata.topic() + " topic", exception);
        return;
      }
      logger.info("SUCCESS! Topic: " + metadata.topic() + ", Timestamp: " + metadata.timestamp() + ", Partition: " + metadata.partition() + ", Offset: " + metadata.offset());
    };

    var record = new ProducerRecord<String, String>(topic, key, value);
    producer.send(record, callback).get();

  }

  private static Properties properties() {
    var properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    return properties;
  }

  @Override
  public void close() {
    producer.close();
  }
}
