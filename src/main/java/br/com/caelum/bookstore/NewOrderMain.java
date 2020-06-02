package br.com.caelum.bookstore;

import org.apache.kafka.clients.producer.*;
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
//    String order = "765,Yesod,Alexandre Oliveira et al,29.90";
//    String order = "432,Metricas Ageis,Raphael Albino,29.90";
    String order = "213,PostgreSQL,Vinicius Carvalho,29.90";

    var producer = new KafkaProducer<String, String>(properties());

    Callback callback = (RecordMetadata metadata, Exception exception) -> {
      if (exception != null) {
        logger.error("ERROR when sending record to " + metadata.topic() + " topic", exception);
        return;
      }
      logger.info("SUCCESS! Topic: " + metadata.topic() + ", Timestamp: " + metadata.timestamp() + ", Partition: " + metadata.partition() + ", Offset: " + metadata.offset());
    };

    var orderRecord = new ProducerRecord<String, String>("BOOK_ORDERS", order);
    //Future<RecordMetadata> future =
    producer.send(orderRecord, callback).get();

    //RecordMetadata result = future.get();
    //System.out.println("SUCCESS! Topic: " + result.topic() + ", Timestamp: " + result.timestamp() + ", Partition: " + result.partition() + ", Offset: " + result.offset());

    var email = "Thank you for your order. The gerbils at the Bookstore have just finished hand-crafting your eBook: " + order;
    var emailRecord = new ProducerRecord<String, String>("BOOK_EMAILS", email);
    producer.send(emailRecord, callback).get();

  }

  private static Properties properties() {
    var properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    return properties;
  }

}
