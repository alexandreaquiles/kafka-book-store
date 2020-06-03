package br.com.caelum.bookstore;

import java.util.List;

public class OrderFinished {

  private String userId;
  private List<Book> books = List.of();
  private String orderId;
//  private LocalDate timestamp;

  public OrderFinished() {
  }

  public OrderFinished(String userId, List<Book> books, String orderId) {
    this.userId = userId;
    this.books = books;
    this.orderId = orderId;
  }

  public String getUserId() {
    return userId;
  }

  public void setUserId(String userId) {
    this.userId = userId;
  }

  public List<Book> getBooks() {
    return books;
  }

  public void setBook(List<Book> book) {
    this.books = books;
  }

  public String getOrderId() {
    return orderId;
  }

  public void setOrderId(String orderId) {
    this.orderId = orderId;
  }

  @Override
  public String toString() {
    return "OrderFinished{" +
      "userId=" + userId +
      ", books=" + books +
      ", orderId='" + orderId + '\'' +
      '}';
  }
}
