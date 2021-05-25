package com.github.segyhp.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerWithKey {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String bootstrapServers = "127.0.0.1:9092";
        Logger logger = LoggerFactory.getLogger(ProducerWithKey.class);

        //Create producer properties
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 10; i++) {

            String topic = "first_topic";
            String value = "Producer value: " + Integer.toString(i);
            String key = "id_" + Integer.toString(i);

            logger.info("key: " + key);

            /*
                notes for key and partition:
                    - if you dont specify partition number, it always be use partition 0
                    - number of partitions determine allocation of partition used by producer
             */


            //create  producer record
            ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>(topic, key, value);
            //send data -  asynchronous
            producer.send(record, new Callback() {
                //execute everytime record is  successfully sent or throw an exception
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        //the record has been successfully sent
                        logger.info("Received new metadata" + "\n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp()
                        );
                    } else {
                        logger.error("Error while producing", e);
                    }
                }
            }).get(); //block the send to make it synchronous, dont do this in production;
        }

        //flush the data
        producer.flush();

        //flush and close data
        producer.close();
    }
}
