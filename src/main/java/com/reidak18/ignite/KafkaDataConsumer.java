package com.reidak18.ignite;

import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class KafkaDataConsumer
{
    static final String TOPIC = "logs";
    static final String GROUP = "logs_group";

    public void ReceiveMessage()
    {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", GROUP);
        props.setProperty("auto.commit.interval.ms", "1000");

//        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);)
//        {
//
//        }
    }
}