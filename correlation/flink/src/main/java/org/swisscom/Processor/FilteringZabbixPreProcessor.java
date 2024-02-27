package org.swisscom.Processor;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.swisscom.POJOs.FilteredEvent_POJO;
import org.swisscom.POJOs.ServiceMonitoringOutput_POJO;
import org.swisscom.POJOs.ZabbixEvents_POJO;

public class FilteringZabbixPreProcessor extends ProcessFunction<ZabbixEvents_POJO, FilteredEvent_POJO> {
    @Override
    public void processElement(ZabbixEvents_POJO zabbixEventsPojo, ProcessFunction<ZabbixEvents_POJO, FilteredEvent_POJO>.Context context, Collector<FilteredEvent_POJO> collector) throws Exception {
        System.out.println("From Filtering Event: "+zabbixEventsPojo);
    }
}
