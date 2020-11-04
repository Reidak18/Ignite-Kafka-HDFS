package com.reidak18.ignite;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaDataProducer
{
    Properties props = new Properties();

    public KafkaDataProducer() throws ExecutionException, InterruptedException {
    }

    public void Init() throws InterruptedException
    {
        props.setProperty("bootstrap.servers", "localhost:9082");
        props.setProperty("kafka.topic.name", "logs");

        KafkaProducer<String, byte[]> producer = new KafkaProducer<String, byte[]>(this.props, new StringSerializer(), new ByteArraySerializer());

        byte[] message = ("Отправленное сообщение").getBytes();
        ProducerRecord<String, byte[]> record = new ProducerRecord<String, byte[]>(props.getProperty("kafka.topic.name"), message);
        producer.send(record);
        System.out.println("Сообщение отправляется");
        Thread.sleep(1000);
        System.out.println("Сообщение отправлено");
        producer.close();
    }
}
