package com.reidak18.ignite;

import org.apache.ignite.Ignition;

import java.util.concurrent.ExecutionException;

public final class App
{
    private App() 
    {
        
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
//        Ignition.start();

        System.out.println("Waiting data from Kafka...");
        KafkaDataConsumer consumer = new KafkaDataConsumer();
        consumer.ReceiveMessage();
    }
}
