package com.sriwin.kafka.model;

import lombok.*;

@Getter
@Setter
public class User {
  private String fName;
  private String lName;

  public User(String fName, String lName) {
    this.fName = fName;
    this.lName = lName;
  }

  @Override
  public String toString() {
    return fName + "-" + lName;
  }
}