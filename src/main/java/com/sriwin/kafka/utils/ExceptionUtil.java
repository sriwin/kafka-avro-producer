package com.sriwin.kafka.utils;

import com.sriwin.kafka.exception.CoreException;

public class ExceptionUtil {


  public static CoreException handleCoreException(Exception ex, String appErrorMsg) {
    if (!(ex instanceof CoreException)) {
      return new CoreException(appErrorMsg, ex);
    }
    return (CoreException) ex;
  }

  public static String getErrorMsg(Exception ex) {
    return ex.toString();
  }
}
