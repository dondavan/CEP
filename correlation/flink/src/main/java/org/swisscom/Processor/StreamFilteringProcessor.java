package org.swisscom.Processor;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.swisscom.POJOs.TCI_POJO;
import org.swisscom.POJOs.Zabbix_events_POJO;

public class StreamFilteringProcessor extends ProcessFunction<TCI_POJO ,TCI_POJO > {

    public void processElement(TCI_POJO s, ProcessFunction<TCI_POJO, TCI_POJO>.Context context, Collector<TCI_POJO> collector) throws Exception {
        System.out.println(s);
    }

}