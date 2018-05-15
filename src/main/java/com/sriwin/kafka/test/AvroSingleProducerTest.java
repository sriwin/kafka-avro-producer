package com.sriwin.kafka.test;

import com.sriwin.kafka.driver.DriverScript;
import com.sriwin.kafka.exception.CoreException;
import com.sriwin.kafka.service.ProduceService;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class AvroSingleProducerTest extends DriverScript {
  private static final Logger logger = LoggerFactory.getLogger(AvroSingleProducerTest.class.getName());

  private static final String TEST_DATA_FILE = "user-test-data.json";
  private static final String SCHEMA_FILE = "schema/user.avsc";
  private static final String TOPIC_NAME = "user_topic";

  public static void main(String[] args) {
    try {
      AvroSingleProducerTest avroSingleProducerTest = new AvroSingleProducerTest();
      avroSingleProducerTest.doInit();
      avroSingleProducerTest.sendEvent();

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void sendEvent() {
    try {
      // generate unique id
      String uniqueId = UUID.randomUUID().toString();

      // prepare GenericRecord Object
      GenericRecord genericRecord = genericRecord();

      // prepare ProducerRecord
      ProducerRecord<String, GenericRecord> producerRecord = new ProducerRecord<String, GenericRecord>(
          TOPIC_NAME, uniqueId, genericRecord);
      logger.info("ProducerRecord : " + producerRecord.toString());

      // post event onto Topic
      ProduceService produceService = new ProduceService();
      //produceService.sendMsg(producerRecord);

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private GenericRecord genericRecord() throws CoreException {
    try {
      // read schema file as string
      String schemaFile = getResourceFileAsString(SCHEMA_FILE);
      Schema schema = new Schema.Parser().parse(schemaFile);

      // read json data
      String jsonStr = getResourceFileAsString(TEST_DATA_FILE);

      // convert json data to generic record
      DecoderFactory decoderFactory = new DecoderFactory();
      Decoder decoder = decoderFactory.jsonDecoder(schema, jsonStr);
      DatumReader<GenericData.Record> reader = new GenericDatumReader<>(schema);
      GenericRecord genericRecord = reader.read(null, decoder);
      logger.info("Generic Record : " + genericRecord.toString());

      return genericRecord;

    } catch (Exception e) {
      throw new CoreException(e);
    }
  }
}