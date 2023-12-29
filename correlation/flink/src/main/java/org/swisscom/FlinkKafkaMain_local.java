package org.swisscom;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.swisscom.Pipeline.NQAPipeline;
import org.swisscom.Pipeline.ZabbixPipeline;
import org.swisscom.Pipeline.ZabbixTCIJointPipeline;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;


/*
*    This is the entry point for flink event correlation
*    Read in kafka configuration and fire up connector to kafka
* */
public class FlinkKafkaMain_local {

    public static void main(String[] args) {

        /* Topics that consumer subscribe to */
        List<String> topics = new ArrayList<String>(Arrays.asList("Zabbix_events","TCI","nqa_raw"));

        /* Consumer group ID */
        String groupID = "Test-group";

        /* Reads in configuration property and phase it*/
        String configFilePath = "src/main/resources/conf/kafka_local.properties";

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

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Collection<String> toTe1 = new ArrayList<String>(Collections.singletonList(topics.get(0)));
        Collection<String> toTe2 = new ArrayList<String>(Collections.singletonList(topics.get(1)));
        Collection<String> toTe3 = new ArrayList<String>(Collections.singletonList(topics.get(2)));

        ZabbixPipeline zabbixPipeline = new ZabbixPipeline(prop,toTe1,groupID,env);
        zabbixPipeline.createKafkaSource();

        ZabbixTCIJointPipeline zabbixTCIJointPipeline = new ZabbixTCIJointPipeline(prop,toTe2,groupID,env);
        zabbixTCIJointPipeline.createKafkaSource();

        NQAPipeline nqaPipeline = new NQAPipeline(prop,toTe3,groupID,env);
        nqaPipeline.createKafkaSource();

        /* Execute data stream process*/
        try{
            env.execute("Event Correlation");
        }catch (Exception e){
            System.out.println(e.getMessage());
        }


    }

}
