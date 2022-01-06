package com.github.KKghub.kafka.tutorial1;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

@Slf4j
public class ProducerDemoWithCallback {

    public static void main(String[] args) {
        String bootstrapServers = "127.0.0.1:9092";

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // create a Producer record

        // send data - async
        for (int i=0; i < 10; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "hello world " + i);

            producer.send(record, (metadata, exception) -> {
                // executes everytime a record is successfully sent or an exception is thrown
                if (exception == null) {
                    // the record was successfully sent
                    log.info("Received metadata: \n" +
                            "Topic: " + metadata.topic() + "\n" +
                            "Partition: " + metadata.partition() + "\n" +
                            "Offset: " + metadata.offset() + "\n" +
                            "Timestamp: " + metadata.timestamp());
                } else {
                    log.error("Error while producing", exception);
                }
            });
        }


        // force all data to be produced
        producer.flush();

        // flush and close producer
        producer.close();
    }
}
