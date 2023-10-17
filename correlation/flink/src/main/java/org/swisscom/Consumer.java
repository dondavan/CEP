package org.swisscom;

import org.apache.kafka.clients.consumer.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

public class Consumer {

    private Properties pros = new Properties();
    /* List of topic subscribed to  */
    private Collection<String> topics;

    public Consumer(Properties pros,Collection<String> topics){
        this.pros = pros;
        this.topics = topics;
    }

    public void pollConsume(){

        try(KafkaConsumer<String, String> consumer = new KafkaConsumer<>(this.pros)){
            consumer.subscribe(this.topics);

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    String key = record.key();
                    String value = record.value();
                    System.out.println(value);
                }
            }

        }

    }


}
