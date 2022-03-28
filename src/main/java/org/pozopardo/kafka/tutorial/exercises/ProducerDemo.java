package org.pozopardo.kafka.tutorial.exercises;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {

    public static void main(String [] args) {
        System.out.println("Hello, Worldy!!");

        // 1. Create producer properties
        String bootstrapServer = "127.0.0.1:9092";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            StringSerializer.class.getName());

        // 2. Create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // 3. Create producer record
        ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "hello world");
        // 4. Send datal
        producer.send(record);

        // 5. Flush data and close
        producer.flush();
        producer.close();
    }
}
