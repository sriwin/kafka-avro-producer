package com.sriwin.kafka.service;

import com.sriwin.kafka.utils.AppConstants;
import com.sriwin.kafka.utils.PropertyManager;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProduceService {
  private static final Logger logger = LoggerFactory.getLogger(ProduceService.class.getName());

  public void sendMsg(ProducerRecord producerRecord) {
    logger.info("############################################### ");
    logger.info("#### in ProduceService.sendMsg()");
    logger.info("############################################### ");
    logger.info("ProducerRecord = " + producerRecord);

    PropertyManager propertyManager = PropertyManager.getInstance();
    String key = System.currentTimeMillis() + "";
    Producer<String, String> producer = getKafkaProducer();
    producer.send(producerRecord);
    logger.info("Message sent successfully");
    producer.flush();
    producer.close();
  }

  private Producer getKafkaProducer() {
    PropertyManager propertyManager = PropertyManager.getInstance();

    Properties props = new Properties();
    props.put("buffer.memory", propertyManager.getValue(AppConstants.KAFKA_PRODUCER_BUFFER_MEMORY));
    props.put("bootstrap.servers", propertyManager.getValue(AppConstants.KAFKA_BOOTSTRAP_SERVER));
    props.put("value.serializer", propertyManager.getValue(AppConstants.KAFKA_VALUE_SERIALIZER));
    props.put("batch.size", propertyManager.getValue(AppConstants.KAFKA_PRODUCER_BATCH_SIZE));
    props.put("key.serializer", propertyManager.getValue(AppConstants.KAFKA_KEY_SERIALIZER));
    props.put("retries", propertyManager.getIntValue(AppConstants.KAFKA_PRODUCER_RETRIES));
    props.put("acks", propertyManager.getValue(AppConstants.KAFKA_PRODUCER_ACKS));

    Producer<String, String> producer = new KafkaProducer<String, String>(props);
    return producer;
  }
}