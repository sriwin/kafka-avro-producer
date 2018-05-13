package com.sriwin.kafka.model;

import lombok.*;

@Getter
@Setter

public class User {
  private String firstName;
  private String lastName;
  private String id;

  public User(String id, String firstName, String lastName) {
    this.firstName = firstName;
    this.lastName = lastName;
    this.id = id;
  }

  @Override
  public String   toString() {
    return "User{" +
        "firstName='" + firstName + '\'' +
        ", lastName='" + lastName + '\'' +
        ", id='" + id + '\'' +
        '}';
  }
}
