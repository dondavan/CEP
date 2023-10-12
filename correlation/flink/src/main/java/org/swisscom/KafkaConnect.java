package org.swisscom;

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
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        System.out.println(prop);

        Consumer consumer = new Consumer(prop);
        consumer.pollConsume();

    }

}
