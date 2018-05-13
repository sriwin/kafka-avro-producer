package com.sriwin.kafka.utils;

public interface AppConstants {
  String PROPS_FILE = "config.properties";

  /**
   * kafka  - base properties
   */
  String KAFKA_TOPIC = "kafka.topic";
  String KAFKA_BOOTSTRAP_SERVER = "kafka.bootstrap.servers";
  String KAFKA_KEY_SERIALIZER = "kafka.key.serializer";
  String KAFKA_VALUE_SERIALIZER = "kafka.value.serializer";

  /**
   * Kafka producer properties
   */
  String KAFKA_PRODUCER_ACKS = "kafka.producer.acks";
  String KAFKA_PRODUCER_RETRIES = "kafka.producer.retries";
  String KAFKA_PRODUCER_BATCH_SIZE = "kafka.producer.batch.size";
  String KAFKA_PRODUCER_BUFFER_MEMORY = "kafka.producer.buffer.memory";
}
