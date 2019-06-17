package com.zharker;

import avro.shaded.com.google.common.collect.Maps;
import com.zharker.serialize.Customer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class MainTest {

    private static final String BOOTSTRAP_SERVERS = "192.168.1.106:9092";

    public static void main(String[] args){

        consumerRecieveDataWithoutConsumerGroupTest();

//        consumerRecieveAvroObjectTest();
        
//        consumerRecieveObjectTest();

//        comsumerBooktest();

//        producerSendAvroObjectTest();

//        producerSendStringTest();

//        producerSendObjectTest();
    }

    private static void consumerRecieveDataWithoutConsumerGroupTest() {
        Properties kafkaPros = new Properties();
        kafkaPros.put("bootstrap.servers",BOOTSTRAP_SERVERS);
        kafkaPros.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
//        kafkaPros.put("group.id","cg1");
        kafkaPros.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(kafkaPros);

        consumer.assign(consumer.partitionsFor("tp1").stream()
                .map(partitionInfo->new TopicPartition(partitionInfo.topic(),partitionInfo.partition()))
                .collect(Collectors.toList()));

        Thread mainthread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run(){
                consumer.wakeup();
                try{
                    mainthread.join();
                }catch (InterruptedException e){
                    e.printStackTrace();
                }
            }
        });
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10 * 1000l));
                records.forEach(record -> {
                    System.out.println("key:" + record.key() + ",value:" + record.value());
                });
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            consumer.close();
        }
    }

    private static void consumerRecieveAvroObjectTest() {
        Properties kafkaPros = new Properties();
        kafkaPros.put("bootstrap.servers",BOOTSTRAP_SERVERS);
        kafkaPros.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        kafkaPros.put("group.id","cg1");
        kafkaPros.put("value.deserializer","io.confluent.kafka.serializers.KafkaAvroDeserializer");
        kafkaPros.put("schema.registry.url","http://localhost:8081");
        kafkaPros.put("specific.avro.reader", true);
        KafkaConsumer<String,Customer> consumer = new KafkaConsumer<>(kafkaPros);
        consumer.subscribe(Collections.singleton("tp1"));
        Thread thread = new Thread(()->{
            try{
                while(true){
                    Thread.sleep(10*1000);
                    ConsumerRecords<String, Customer> records = consumer.poll(Duration.ofMillis(10*1000l));
                    records.forEach(record->{
                        System.out.println("[to consumer]key:"+record.key());
                        System.out.println("[to consumer]value: "+record.value());
                    });
                }
            }catch (Exception e){
                System.out.println("schema not found exception?");
                e.printStackTrace();
            }finally {
                consumer.close();
            }
        });
        thread.start();
        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run(){
                consumer.wakeup();
                try{
                    thread.join();
                }catch (InterruptedException e){
                    e.printStackTrace();
                }
            }
        });
    }

    private static void consumerRecieveObjectTest() {
        Properties kafkaPros = new Properties();
        kafkaPros.put("bootstrap.servers",BOOTSTRAP_SERVERS);
        kafkaPros.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        kafkaPros.put("value.deserializer","com.zharker.serialize.CustomerDeserializer");
        kafkaPros.put("group.id","cg1");

        KafkaConsumer<String,Customer> consumer = new KafkaConsumer<>(kafkaPros);
        consumer.subscribe(Collections.singleton("tp1"));
        Thread thread = new Thread(()->{
            try{
                while(true){
                    Thread.sleep(10*1000l);
                    System.out.println("===recieve to consumer===");
                    ConsumerRecords<String, Customer> records = consumer.poll(Duration.ofMillis(10*1000l));
                    records.forEach(consumerRecord->{
                        System.out.println("[to consumer]key: "+consumerRecord.key());
                        System.out.println("[to consumer]value: "+consumerRecord.value());

                    });
                }
            }catch (Exception e){
                e.printStackTrace();
            }finally {
                consumer.close();
            }
        });
        thread.start();
        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run(){
                consumer.wakeup();
                try{
                    thread.join();
                }catch (InterruptedException e){
                    e.printStackTrace();
                }
            }
        });
    }

    private static void comsumerBooktest() {

        Properties kafkaPros = new Properties();
        kafkaPros.put("bootstrap.servers",BOOTSTRAP_SERVERS);
        kafkaPros.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        kafkaPros.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        kafkaPros.put("group.id","cg1");
        kafkaPros.put("fetch.min.bytes",100);
        kafkaPros.put("fetch.max.wait,ms",60000);
        kafkaPros.put("enable.auto.commit",false);

        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(kafkaPros);

        Thread mainthread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run(){
                consumer.wakeup();
                try{
                    mainthread.join();
                }catch (InterruptedException e){
                    e.printStackTrace();
                }
            }
        });

        Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadataMap = Maps.newHashMap();
        consumer.subscribe(Collections.singleton("tp1"), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                System.out.println("before re-matching or after close consumer called");
                partitions.forEach(partition->{
                    System.out.println("topic: "+partition.topic()+", partition: "+partition.partition());
                });
                System.out.println("on this method commit offset place");
                consumer.commitSync(topicPartitionOffsetAndMetadataMap);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                System.out.println("after re-matching or before close consumer called");
//                partitions.forEach(partition->{
//                    System.out.println("topic: "+partition.topic()+", partition: "+partition.partition());
//                    System.out.println("seek the offset place: 10");
//                    consumer.seek(partition,10);
//                });
                System.out.println("seek to end test");
                consumer.seekToEnd(partitions);
            }
        });
//        consumer.subscribe(Pattern.compile("tp1"));
        try{
         while (true) {
             /*
             if(topicPartitionOffsetAndMetadataMap.size()>0){
                 System.out.println("seek to beginning test");
                 consumer.seekToBeginning(topicPartitionOffsetAndMetadataMap.keySet());
             }
            */
             ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(30*1000l));
//             consumer.poll(Duration.of(100l,ChronoUnit.MILLIS))
             records.forEach(record -> {
                 System.out.println("record==>"
                         + " topic:" + record.topic()
                         + ", partition:" + record.partition()
                         + ", offset:" + record.offset()
                         + ", key:" + record.key()
                         + ", value:" + record.value()
                 );
                 long newOffset = record.offset()+1;
                 topicPartitionOffsetAndMetadataMap.put(new TopicPartition(record.topic(),record.partition()),new OffsetAndMetadata(newOffset,"metadata_"+newOffset));
             });
             /*
             try {
                 consumer.commitSync();
             }catch (Exception e){
                 e.printStackTrace();
             }
             */
             consumer.commitAsync(topicPartitionOffsetAndMetadataMap, (offets,exception)->{
                 offets.entrySet().forEach(entry->{
                     System.out.println("tpoicPartition: "+entry.getKey().toString());
                     System.out.println("offset: "+entry.getValue().offset()+", metadata: "+entry.getValue().metadata());
                 });
                 if(exception != null){
                     exception.printStackTrace();
                 }
             });

         }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            try {
                consumer.commitSync(topicPartitionOffsetAndMetadataMap);
            }catch (Exception e){
                e.printStackTrace();
            }finally {
                consumer.close();
            }
        }
    }

    private static void producerSendAvroObjectTest() {

        new Thread(()->{
            Properties kafkaPros = new Properties();
            kafkaPros.put("bootstrap.servers","localhost:9092");
            kafkaPros.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
            kafkaPros.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
            kafkaPros.put("schema.registry.url","http://localhost:8081");
            Producer<String, GenericRecord> producer = new KafkaProducer<>(kafkaPros);
            Schema.Parser parser = new Schema.Parser();
            Schema schema = parser.parse(Customer.CUSTOMER_SCHEMA);

            try {
                while (true) {
                    GenericRecord customer = new GenericData.Record(schema);
                    customer.put("id", RandomUtils.nextInt(1000,9999));
                    customer.put("name", RandomStringUtils.random(8,true,true));
                    customer.put("email", RandomStringUtils.random(8,true,true)+"@zharker.com");

                    ProducerRecord<String, GenericRecord> record = new ProducerRecord<>("tp1", customer);

                    try {
                        Object result = producer.send(record).get();
                        System.out.println(result);
                    } catch (InterruptedException|ExecutionException e) {
                        e.printStackTrace();
                    }
                    Thread.sleep(1*1000);
                }
            }catch (Exception e){
                e.printStackTrace();
            }finally {
                producer.close();
            }
        }).start();


    }

    private static void producerSendObjectTest() {
        Properties kafkaPros = new Properties();
        kafkaPros.put("bootstrap.servers","localhost:9092");
        kafkaPros.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        kafkaPros.put("value.serializer","com.zharker.serialize.CustomerSerializer");
        KafkaProducer producer = new KafkaProducer<String,Customer>(kafkaPros);
        new Thread(()->{
            try {
                while (true) {
                    int randomInt = RandomUtils.nextInt(1000, 9999);
                    String key = "Customer" + randomInt;
                    Customer value = new Customer(randomInt, RandomStringUtils.random(6, true, true));
                    System.out.println("===send from producer===");
                    System.out.println("[from producer]key: " + key);
                    System.out.println("[from producer]value: " + value);
                    ProducerRecord<String, Customer> record = new ProducerRecord<>("tp1", key, value);
                    try {
                        Object result = producer.send(record).get();
                        System.out.println("[from producer]" + result);
                        Thread.sleep(1 * 1000l);
                    } catch (InterruptedException | ExecutionException e) {
                        e.printStackTrace();
                    }
                }
            }catch (Exception e){
                e.printStackTrace();
            }finally {
                producer.close();
            }
        }).start();
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
