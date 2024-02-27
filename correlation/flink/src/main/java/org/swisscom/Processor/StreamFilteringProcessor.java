package org.swisscom.Processor;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.swisscom.POJOs.TCI_POJO;

public class StreamFilteringProcessor extends ProcessFunction<TCI_POJO ,TCI_POJO > {

    public void processElement(TCI_POJO tci_pojo, ProcessFunction<TCI_POJO, TCI_POJO>.Context context, Collector<TCI_POJO> collector) throws Exception {
        /*
        * Event is reported every 5 minutes ?
        * Do TCI and Zabbix share the same window start time and size? - This is for choosing flink window implementation like slide
        * */
    }

}