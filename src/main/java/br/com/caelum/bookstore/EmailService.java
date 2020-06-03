package br.com.caelum.bookstore;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class EmailService {

  public static void main(String[] args) {
    var emailService = new EmailService();
    try(var service = new KafkaService(EmailService.class.getSimpleName(),
                                  "BOOK_EMAILS",
                                        emailService::parse,
                                        String.class)) {
          service.run();
    }
  }

  private void parse(ConsumerRecord<String, String> record) {
    String value = record.value();
    System.out.println("Sending email: " + value);
    try {
      Thread.sleep(500);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    System.out.println("Email sent: " + value);
  }
}
