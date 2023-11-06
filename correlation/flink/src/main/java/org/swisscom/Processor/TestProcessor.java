package org.swisscom.Processor;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/* Example process for event in datasource */
public class TestProcessor extends ProcessFunction<String,String> {

    @Override
    public void processElement(String s, ProcessFunction<String, String>.Context context, Collector<String> collector) throws Exception {
        System.out.println(s);
    }

}
