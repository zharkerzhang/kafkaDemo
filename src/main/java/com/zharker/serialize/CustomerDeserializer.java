package com.zharker.serialize;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.ByteBuffer;
import java.util.Map;

public class CustomerDeserializer implements Deserializer<Customer> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public Customer deserialize(String topic, byte[] data) {

        int id = 0;
        int nameSize;
        String name = null;
        try{
            if(data==null){
                return null;
            }
            if(data.length<8){
                throw new SerializationException("size of data error");
            }
            ByteBuffer byteBuffer = ByteBuffer.wrap(data);
            id=byteBuffer.getInt();
            nameSize=byteBuffer.getInt();
            byte[] nameBytes = new byte[nameSize];
            byteBuffer.get(nameBytes);
            name = new String(nameBytes,"UTF-8");
        }catch (Exception e){
            e.printStackTrace();
        }

        return new Customer(id, name);
    }

    @Override
    public void close() {

    }
}
