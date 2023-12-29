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
        /*
        1.Check for alerts that are closed within the timeframe,and consider filtering them out. The alert is a non-issue,
        and it is likely just bad network conditions not a failure
        2.Check for alerts that have been open for over a day, and filter these out too. The alert is a long-standing failure, likely because a customer has disconnected their site, not a system failure.
        3.Determine the threshold based on previous data instead of on a static threshold
         */

        // retrieve the current state
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