package br.com.caelum.bookstore;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

  public static void main(String[] args) throws ExecutionException, InterruptedException {

    try(var orderDispatcher = new KafkaDispatcher();
        var emailDispatcher = new KafkaDispatcher()) {

      String order1 = "123,Clean Code,Robert Uncle Bob Martin,159.90";
      String order2 = "456,Git,Alexandre e Rodrigo,29.90";
      String order3 = "789,Clojure,Gregório Melo,29.90";
      String order4 = "098,Haskell,Alexandre Oliveira,29.90";
      String order5 = "765,Yesod,Alexandre Oliveira et al,29.90";
      String order6 = "432,Metricas Ageis,Raphael Albino,29.90";
      String order7 = "213,PostgreSQL,Vinicius Carvalho,29.90";

      List<String> orders = List.of(order1, order2, order3, order4, order5, order6, order7);
      for (String order : orders) {
        var userId = UUID.randomUUID().toString();
        orderDispatcher.send("BOOK_ORDERS", userId, order + "," + userId);

        var email = "Thank you for your order. The gerbils at the Bookstore have just finished hand-crafting your eBook: " + (order + "," + userId);
        emailDispatcher.send("BOOK_EMAILS", userId, email);
      }
    }
  }

}
