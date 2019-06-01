package com.zharker.serialize;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;

public class CustomerSerializer implements Serializer<Customer> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {}

    @Override
    public byte[] serialize(String topic, Customer data) {
        byte[] serializeName;
        int stringSize;
        if(data==null){
            return null;
        }
        if(data.getCustomerName()!=null){
            try {
                serializeName = data.getCustomerName().getBytes("UTF-8");
                stringSize = serializeName.length;
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
                throw new SerializationException("error when serializing customer: "+e);
            }

        }else{
            serializeName = new byte[0];
            stringSize = 0;
        }
        ByteBuffer buffer = ByteBuffer.allocate(4+4+stringSize);
        buffer.putInt(data.getCustomerId());
        buffer.putInt(stringSize);
        buffer.put(serializeName);
        return buffer.array();
    }

    @Override
    public void close() {}
}
