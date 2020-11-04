package com.reidak18.kafkaprovider;

import java.util.concurrent.ExecutionException;

public final class App
{
    private App()
    {

    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        KafkaDataProducer producer = new KafkaDataProducer();
        producer.Init();
    }
}

