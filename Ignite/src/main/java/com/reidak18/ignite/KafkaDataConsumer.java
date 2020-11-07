package com.reidak18.ignite;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

// Класс для приема сообщений от KafkaProvider
public class KafkaDataConsumer
{
    static final String TOPIC = "logs";
    static final String GROUP = "logs_group";

    public void ReceiveMessage() throws InterruptedException
    {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", GROUP);
        props.setProperty("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        try
        {
            KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
            consumer.subscribe(Arrays.asList(TOPIC));

            // Костыль: считываем вхолостую все, что было в очереди до отправки
            ConsumerRecords<String, String> records = consumer.poll(1000L);

            // Ждем сообщения
            while(true)
            {
                records = consumer.poll(1000L);
                for (ConsumerRecord<String, String> record : records)
                {
                    // Сохраняем полученные данные в HDFS
                    HDFSWriter writer = new HDFSWriter();
                    writer.SaveInputToHDFS(record.value());
                }
                // Как только пришло - останавливаемся
                if (records.count() != 0)
                    break;
            }
        }
        catch (Exception ex)
        {
            System.out.println("Exception: " + ex.getMessage());
        }
    }
}


