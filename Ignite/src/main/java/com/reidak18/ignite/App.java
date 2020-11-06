package com.reidak18.ignite;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.compute.ComputeTaskAdapter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;

public final class App
{
    private App() 
    {
        
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {

        System.out.println("Waiting data from Kafka...");
        KafkaDataConsumer consumer = new KafkaDataConsumer();
//        consumer.ReceiveMessage();
        String str = consumer.ReadFromFile("C:\\Users\\Nikita\\Downloads\\messages");

        try (Ignite ignite = Ignition.start())
        {
            System.out.println();
            System.out.println("Compute task map example started.");

            ArrayList logs = ignite.compute().execute(IgniteMapperReducer.class, str);

            System.out.println();
            for (Object log: logs)
            {
                HourLog hourLog = (HourLog) log;
                System.out.println(hourLog.Print());
            }
        }
    }
}


