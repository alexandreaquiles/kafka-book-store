package br.com.caelum.bookstore;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class EbookGeneratorService {

  public static void main(String[] args) {
    var ebookGeneratorService = new EbookGeneratorService();
    try(var service = new KafkaService(EbookGeneratorService.class.getSimpleName(),
                                  "BOOK_ORDERS",
                                        ebookGeneratorService::parse,
                                        OrderFinished.class)) {
      service.run();
    }
  }

  private void parse(ConsumerRecord<String, OrderFinished> record) {
    OrderFinished value = record.value();
    System.out.println("Generating ebook: " + value);
    try {
      Thread.sleep(5000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    System.out.println("Ebook generated: " + value);

  }
}
