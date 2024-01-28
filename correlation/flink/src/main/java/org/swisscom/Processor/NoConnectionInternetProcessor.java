package org.swisscom.Processor;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.swisscom.POJOs.Aggregation_Alert_POJO;
import org.swisscom.POJOs.Zabbix_events_POJO;
import org.swisscom.States.NoConnectionInternetState;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

public class NoConnectionInternetProcessor extends ProcessWindowFunction<Zabbix_events_POJO, Aggregation_Alert_POJO, String, TimeWindow> {

    /* Count the occurrence of type of event*/
    private ValueState<NoConnectionInternetState> countState;

    @Override
    public void open(Configuration parameters) throws Exception {
        countState = getRuntimeContext().getState(new ValueStateDescriptor<>("NoConnectionInternetState", NoConnectionInternetState.class));
    }

    @Override
    public void process(String s, ProcessWindowFunction<Zabbix_events_POJO, Aggregation_Alert_POJO, String, TimeWindow>.Context context, Iterable<Zabbix_events_POJO> iterable, Collector<Aggregation_Alert_POJO> collector) throws Exception {

        /* Retrieve the current state */
        NoConnectionInternetState current = this.countState.value();
        if (current == null) {
            current = new NoConnectionInternetState();
        }

        Aggregation_Alert_POJO aggregationAlertPojo = new Aggregation_Alert_POJO();

        /* Parse timestamp */
        Instant windowStart = Instant.ofEpochMilli(context.window().getStart());
        Instant windowEnd = Instant.now(); /*End of current window counting which is now*/
        windowEnd = windowEnd.truncatedTo(ChronoUnit.SECONDS);

        aggregationAlertPojo.alert_type     = "NQA_PROBES_INTERNET";
        aggregationAlertPojo.count          = current.count;
        aggregationAlertPojo.window_start   = windowStart.toString();
        aggregationAlertPojo.window_end     = windowEnd.toString();

        collector.collect(aggregationAlertPojo);

    }

}