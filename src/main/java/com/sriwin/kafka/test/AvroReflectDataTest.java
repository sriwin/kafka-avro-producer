package com.sriwin.kafka.test;

import com.sriwin.kafka.driver.DriverScript;
import com.sriwin.kafka.exception.CoreException;
import com.sriwin.kafka.model.User;
import com.sriwin.kafka.service.ProduceService;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.UUID;

public class AvroReflectDataTest extends DriverScript {
  private static final Logger logger = LoggerFactory.getLogger(AvroReflectDataTest.class.getName());

  private static final String TEST_DATA_FILE = "reflectdata-test-data.json";
  private static final String TOPIC_NAME = "user_topic";

  public static void main(String[] args) {
    try {
      AvroReflectDataTest avroReflectDataTest = new AvroReflectDataTest();
      avroReflectDataTest.doInit();
      avroReflectDataTest.sendEvent();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void sendEvent() {
    try {
      // generate unique id
      String uniqueId = UUID.randomUUID().toString();

      // prepare GenericRecord Object
      GenericData.Array<User> genericDataRecordArray = genericRecord();

      for (Iterator iterator = genericDataRecordArray.iterator(); iterator.hasNext(); ) {
        User genericRecord = (User) iterator.next();
        logger.info("GenericRecord : " + genericRecord);

        // prepare ProducerRecord
        ProducerRecord<String, User> producerRecord = new ProducerRecord<String, User>(
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


  private GenericData.Array<User> genericRecord() throws CoreException {
    try {
      Schema schema = ReflectData.get().getSchema(User.class);

      // read json data
      String jsonStr = getResourceFileAsString(TEST_DATA_FILE);

      Decoder decoder = new DecoderFactory().jsonDecoder(schema, jsonStr);
      DatumReader<GenericData.Array<User>> reader = new GenericDatumReader<>(schema);
      GenericData.Array<User> parsedRecords = null;

      while (true) {
        try {
          parsedRecords = reader.read(null, decoder);
        } catch (Exception e) {
          e.printStackTrace();
          break;
        }
      }
      return parsedRecords;

    } catch (Exception e) {
      throw new CoreException(e);
    }
  }
}
