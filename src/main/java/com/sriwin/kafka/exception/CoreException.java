package com.sriwin.kafka.exception;

public class CoreException extends Exception {
  private String appErrorMsg;

  public CoreException() {
  }

  public CoreException(String message) {
    super(message);
  }

  public CoreException(String message, Throwable cause) {
    super(cause);
    this.appErrorMsg = message;
  }

  public CoreException(Throwable cause) {
    super(cause);
  }


  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append(appErrorMsg + "<br/>");
    builder.append(getMessage() + "<br/>");
    return builder.toString();
  }
}
