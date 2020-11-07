package com.reidak18.kafkaprovider;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.ExecutionException;

public final class App
{
    private App()
    {

    }

    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
        // /root/Desktop/Ignite-Kafka-HDFS-master/input
        KafkaDataProducer producer = new KafkaDataProducer(ReadFromFile(args[0]));
        producer.Init();
    }

    public static String ReadFromFile(String path) throws IOException
    {
        String contents = "";
        File dir = new File(path);

        for (File file : dir.listFiles()) {
            contents += new String(Files.readAllBytes(Paths.get(String.valueOf(Paths.get(path, file.getName())))));
        }

        return contents;
    }
}
