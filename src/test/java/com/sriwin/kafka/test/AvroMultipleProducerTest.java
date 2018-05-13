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

import java.util.Iterator;
import java.util.UUID;

public class AvroMultipleProducerTest extends DriverScript {
  private static final Logger logger = LoggerFactory.getLogger(ProduceService.class.getName());

  private static final String TEST_DATA_FILE = "users-test-data.json";
  private static final String SCHEMA_FILE = "schema/users.avsc";
  private static final String TOPIC_NAME = "user_topic";

  public static void main(String[] args) {
    try {
      AvroMultipleProducerTest avroSingleProducerTest = new AvroMultipleProducerTest();
      avroSingleProducerTest.doInit();
      avroSingleProducerTest.sendEvent();

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void sendEvent() {
    try {
      // prepare GenericRecord Object
      GenericData.Array<GenericData.Record> genericDataRecordArray = genericRecord();

      for (Iterator iterator = genericDataRecordArray.iterator(); iterator.hasNext(); ) {
        // generate unique id
        String uniqueId = UUID.randomUUID().toString();

        GenericData.Record genericRecord = (GenericData.Record) iterator.next();
        logger.info("GenericRecord : " + genericRecord);


        // prepare ProducerRecord
        ProducerRecord<String, GenericRecord> producerRecord = new ProducerRecord<String, GenericRecord>(
            TOPIC_NAME, uniqueId, genericRecord);
        logger.info("ProducerRecord : " + producerRecord.toString());

        // post event onto Topic
        ProduceService produceService = new ProduceService();
        //produceService.sendMsg(producerRecord);
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private GenericData.Array<GenericData.Record> genericRecord() throws CoreException {
    try {
      // read schema file as string
      String schemaFile = getResourceFileAsString(SCHEMA_FILE);
      Schema schema = new Schema.Parser().parse(schemaFile);

      // read json data
      String jsonStr = getResourceFileAsString(TEST_DATA_FILE);

      // convert json data to generic record
      DecoderFactory decoderFactory = new DecoderFactory();
      Decoder decoder = decoderFactory.jsonDecoder(schema, jsonStr);

      DatumReader<GenericData.Array<GenericData.Record>> datumReader = new GenericDatumReader<>(schema);
      GenericData.Array<GenericData.Record> genericDataRecordArray = datumReader.read(null, decoder);
      GenericData.Record entry = genericDataRecordArray.get(1);
      System.out.println(entry);

      return genericDataRecordArray;

    } catch (Exception e) {
      throw new CoreException(e);
    }
  }
}