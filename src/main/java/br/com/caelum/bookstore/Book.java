package br.com.caelum.bookstore;

public class Book {

  private String name;
  private String authors;
  private Double price;

  public Book () {
  }

  public Book(String name, String authors, Double price) {
    this.name = name;
    this.authors = authors;
    this.price = price;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getAuthors() {
    return authors;
  }

  public void setAuthors(String authors) {
    this.authors = authors;
  }

  public Double getPrice() {
    return price;
  }

  public void setPrice(Double price) {
    this.price = price;
  }

  @Override
  public String toString() {
    return "Book{" +
      "name='" + name + '\'' +
      ", authors='" + authors + '\'' +
      ", price=" + price +
      '}';
  }
}
