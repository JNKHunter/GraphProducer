package com.brightmeta;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by John on 9/19/17.
 */
public class GraphProducer {

    private static final int NODE_LO = 0;
    private static final int NODE_HI = 101;

    private static final int WEIGHT_LO = 0;
    private static final int WEIGHT_HI = 11;

    private Properties properties;
    private Runnable runnable;
    private Random random;

    public GraphProducer(){
        properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        properties.setProperty("acks", "1");
        properties.setProperty("retries", "3");
        properties.setProperty("linger.ms", "1");

        Producer<String, String> producer = new KafkaProducer<String, String>(properties);

        random = new Random();

        runnable = () -> {
            producer.send(new ProducerRecord<String, String>(
                    "test",this.randNode() + "",
                    this.randNode() + "," + this.randWeight()));
        };

        final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        executorService.scheduleAtFixedRate(this.runnable,0,3, TimeUnit.SECONDS);
    }

    private int randNode(){
        return random.nextInt((NODE_HI - NODE_LO) + NODE_LO);
    }

    private int randWeight(){
        return random.nextInt((WEIGHT_HI - WEIGHT_LO) + WEIGHT_LO);
    }
}
