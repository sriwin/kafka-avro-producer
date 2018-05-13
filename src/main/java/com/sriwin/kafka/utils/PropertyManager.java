package com.sriwin.kafka.utils;

import com.sriwin.kafka.exception.CoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

public class PropertyManager {
  private Properties properties = null;
  private static PropertyManager ourInstance = new PropertyManager();
  private static final Logger logger = LoggerFactory.getLogger(PropertyManager.class.getName());

  public static PropertyManager getInstance() {
    return ourInstance;
  }

  public void addProperty(String key, String value) {
    properties.put(key, value);
  }

  public void loadProperties(String filePath) throws CoreException {
    try {
      if (properties == null) {
        properties = new Properties();
      }
      InputStream file = new FileInputStream(new File(filePath));
      properties.load(file);
    } catch (Exception e) {
      throw ExceptionUtil.handleCoreException(e, "Exception occurred while reading the file" + filePath);
    }
  }

  public String getValue(String key) {
    return properties.getProperty(key.trim()).trim();
  }

  public boolean getBooleanValue(String key) {
    String value = getValue(key.trim()).trim();
    if (value != null && value.trim().length() > 0) {
      if (value.equalsIgnoreCase("yes") || value.equalsIgnoreCase("true")) {
        return true;
      } else if (value.equalsIgnoreCase("no") || value.equalsIgnoreCase("false")) {
        return false;
      }
    }
    return false;
  }

  public int getIntValue(String key) {
    String value = properties.getProperty(key.trim()).trim();
    return Integer.parseInt(value);
  }

  public void init() throws CoreException {
    try {
      String userDir = DirUtil.getConfigDir();
      String configFilePath = userDir + AppConstants.PROPS_FILE;
      loadProperties(configFilePath);

    } catch (Exception e) {
      throw ExceptionUtil.handleCoreException(e, "Exception occurred while reading the file" + AppConstants.PROPS_FILE);
    }
  }
}