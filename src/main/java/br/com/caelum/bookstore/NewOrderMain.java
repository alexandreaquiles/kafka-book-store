package br.com.caelum.bookstore;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class NewOrderMain {

  private static final Logger logger = LoggerFactory.getLogger(NewOrderMain.class);

  public static void main(String[] args) throws ExecutionException, InterruptedException {

//    String order = "123,Clean Code,Robert Uncle Bob Martin,159.90";
//    String order = "456,Git,Alexandre e Rodrigo,29.90";
//    String order = "789,Clojure,Gregório Melo,29.90";
//    String order = "098,Haskell,Alexandre Oliveira,29.90";
    String order = "765,Yesod,Alexandre Oliveira et al,29.90";

    var producer = new KafkaProducer<String, String>(properties());

    var record = new ProducerRecord<String, String>("BOOK_ORDERS", order);
    Future<RecordMetadata> future = producer.send(record, (RecordMetadata metadata, Exception exception) -> {
      if (exception != null) {
        logger.error("ERROR when sending record to BOOK_ORDERS topic", exception);
        return;
      }
      logger.info("SUCCESS! Topic: " + metadata.topic() + ", Timestamp: " + metadata.timestamp() + ", Partition: " + metadata.partition() + ", Offset: " + metadata.offset());
    });

    //RecordMetadata result =
    future.get();
    //System.out.println("SUCCESS! Topic: " + result.topic() + ", Timestamp: " + result.timestamp() + ", Partition: " + result.partition() + ", Offset: " + result.offset());

  }

  private static Properties properties() {
    var properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    return properties;
  }

}
