package com.zharker;

import com.zharker.serialize.Customer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class MainTest {

    public static void main(String[] args){

//        producerSendAvroObjectTest();

        System.out.println("github commit test");
//        producerSendStringTest();

//        producerSendObjectTest();
    }

    private static void producerSendAvroObjectTest() {
        Properties kafkaPros = new Properties();
        kafkaPros.put("bootstrap.servers","localhost:9092");
        kafkaPros.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        kafkaPros.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        kafkaPros.put("schema.registry.url","http://localhost:8081");
        Producer<String, GenericRecord> producer = new KafkaProducer<>(kafkaPros);
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(Customer.CUSTOMER_SCHEMA);

        GenericRecord customer = new GenericData.Record(schema);
        customer.put("id", RandomUtils.nextInt(1000,9999));
        customer.put("name", RandomStringUtils.random(8,true,true));
        customer.put("email", RandomStringUtils.random(8,true,true)+"@zharker.com");

        ProducerRecord<String, GenericRecord> record = new ProducerRecord<>("test_topic", customer);

        try {
            Object result = producer.send(record).get();
            System.out.println(result);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }finally {
            producer.close();
        }

    }

    private static void producerSendObjectTest() {
        Properties kafkaPros = new Properties();
        kafkaPros.put("bootstrap.servers","localhost:9092");
        kafkaPros.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        kafkaPros.put("value.serializer","com.zharker.serialize.CustomerSerializer");

        KafkaProducer producer = new KafkaProducer<String,Customer>(kafkaPros);

        ProducerRecord<String, Customer> record = new ProducerRecord<>("test_topic","Customer2333",new Customer(2333,"ejvinea"));
        try {
            Object result = producer.send(record).get();
            System.out.println(result);
        } catch (InterruptedException e) {
            e.printStackTrace();

            String str = "{\n" +
                    "    \"type\": \"record\",\n" +
                    "    \"name\": \"Customer\",\n" +
                    "    \"fields\": [\n" +
                    "        {\"name\": \"id\", \"type\": \"int\"},\n" +
                    "        {\"name\": \"name\",  \"type\": \"string\"},\n" +
                    "        {\"name\": \"email\", \"type\": [\"null\",\"string\"],\"default\":\"null\"}\n" +
                    "    ]\n" +
                    "}";
        } catch (ExecutionException e) {
            e.printStackTrace();
        }finally {
            producer.close();
        }
    }

    private static void producerSendStringTest() {
        Properties kafkaPros = new Properties();
        kafkaPros.put("bootstrap.servers","localhost:9092");
        kafkaPros.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        kafkaPros.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        kafkaPros.put("acks","1");

        KafkaProducer producer = new KafkaProducer<String,String>(kafkaPros);

        ProducerRecord<String,String> record = new ProducerRecord<>("test_topic","test_key","test_value");
        try {
            Object result = producer.send(record).get();
            System.out.println(result);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }finally {
            producer.close();
        }
        /*
        producer.send(record,(recordMetadata,execption)->{
            System.out.println("recordMetadata: "+recordMetadata);
            if(execption != null){
                execption.printStackTrace();
            }
        });
        */
    }
}
