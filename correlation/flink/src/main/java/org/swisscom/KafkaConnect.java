package org.swisscom;

import org.apache.kafka.clients.consumer.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class KafkaConnect {

    public static void main(String[] args) {

        /* Reads in configuration property and phase it*/
        String configFilePath = "src/main/resources/conf/kafka.properties";
        FileInputStream propsInput = null;
        Properties prop;
        try {
            propsInput = new FileInputStream(configFilePath);
            prop = new Properties();
            prop.load(propsInput);
            /* Additional Configuration */
            prop.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka_test");
            prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        System.out.println(prop);

        Consumer consumer = new Consumer(prop);
        consumer.pollConsume();

    }

}
