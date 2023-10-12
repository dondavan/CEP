package org.swisscom;

import org.apache.kafka.clients.consumer.*;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

public class Consumer {

    private Properties pros = new Properties();
    /* List of topic subscribed to  */
    private  final String topic =  "Zabbix_events";

    public Consumer(Properties pros){
        this.pros = pros;
        System.out.println(this.pros);
    }

    public void pollConsume(){
        try(KafkaConsumer<String, String> consumer = new KafkaConsumer<>(this.pros)){
            consumer.subscribe(Arrays.asList(this.topic));
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            System.out.println(records.count());

            /*
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    String key = record.key();
                    String value = record.value();
                    System.out.println(value);
                }
            }

             */
        }

    }


}
