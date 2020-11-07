package com.reidak18.ignite;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.compute.ComputeTaskAdapter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.net.URISyntaxException;

public final class App
{
    private App() 
    {
        
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException, URISyntaxException {

        // Ждем данные от Kafka Provider
        System.out.println("Waiting data from Kafka...");
        KafkaDataConsumer consumer = new KafkaDataConsumer();
        consumer.ReceiveMessage();

        // Читаем исходные данные в HDFS
        HDFSWriter reader = new HDFSWriter();
        String input = reader.ReadFileFromHDFS("/user/root/input/input.txt");
        System.out.println("Message received: ");
        System.out.println(input);

        // Запускаем обработку в Ignite
        try (Ignite ignite = Ignition.start())
        {
            System.out.println();
            System.out.println("Compute task map example started.");

            ArrayList logs = ignite.compute().execute(IgniteMapperReducer.class, input);

            String results = "";
            for (Object log: logs)
            {
                HourLog hourLog = (HourLog) log;
                results += hourLog.Print() + "\n";
            }

            // Сохраняем результаты в HDFS
            HDFSWriter writer = new HDFSWriter();
            writer.SaveResultsToHDFS(results);
        }
    }
}


