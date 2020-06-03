package br.com.caelum.bookstore;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

  public static void main(String[] args) throws ExecutionException, InterruptedException {

    try (var orderDispatcher = new KafkaDispatcher();
         var emailDispatcher = new KafkaDispatcher()) {

      Book cleanCode = new Book("Clean Code", "Uncle Bob", 159.9);
      Book git = new Book("Git", "Alexandre Aquiles; Rodrigo Ferreira", 29.9);

      OrderFinished orderFinished = new OrderFinished(UUID.randomUUID().toString(), List.of(cleanCode, git), "123");

      orderDispatcher.send("BOOK_ORDERS", orderFinished.getUserId(), orderFinished);

      var email = "Thank you for your orderFinished. The gerbils at the Bookstore have just finished hand-crafting your eBook: " + orderFinished;
      emailDispatcher.send("BOOK_EMAILS", orderFinished.getUserId(), email);
    }
  }
}

