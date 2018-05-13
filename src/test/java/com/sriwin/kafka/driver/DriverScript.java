package com.sriwin.kafka.driver;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.util.StatusPrinter;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.sriwin.kafka.exception.CoreException;
import com.sriwin.kafka.utils.DirUtil;
import com.sriwin.kafka.utils.PropertyManager;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;

public class DriverScript {
  public void doInit() {
    /**
     * initialize log4j
     */
    LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
    try {
      String logbackFilePath = DirUtil.getConfigDir() + "logback.xml";

      JoranConfigurator configurator = new JoranConfigurator();
      configurator.setContext(context);
      context.reset();
      configurator.doConfigure(logbackFilePath);
    } catch (Exception je) {
      // StatusPrinter will handle this
    }
    StatusPrinter.printInCaseOfErrorsOrWarnings(context);

    try {

      PropertyManager propertyManager = PropertyManager.getInstance();
      propertyManager.init();
    } catch (CoreException e) {
      e.printStackTrace();
    }
  }

  protected static String getResourceFileAsString(String resourceFile) {
    try {
      URL url = Resources.getResource(resourceFile);
      return Resources.toString(url, Charsets.UTF_8);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

}
