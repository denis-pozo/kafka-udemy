package org.pozopardo.kafka.tutorial;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithCallbacksDemo {

    private static final Logger logger = LoggerFactory.getLogger(ProducerWithCallbacksDemo.class);

    public static void main(String [] args) {
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

        for (int i = 0; i < 10; i++) {
            // 3. Create producer record
            ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "hello "
                + "world" + Integer.toString(i));

            // 4. Send data - async
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e == null) {
                        logger.info("Received new metadata. \n" +
                            "Topic: " + recordMetadata.topic()  + "\n" +
                            "Partition: " + recordMetadata.partition()  + "\n" +
                            "Offset: " + recordMetadata.offset()  + "\n" +
                            "Timestamp: " + recordMetadata.timestamp()  + "\n");
                    } else {
                        logger.error("Error while producing", e);
                    }
                }
            });

        }

        // 5. Flush data and close
        producer.flush();
        producer.close();
    }
}
