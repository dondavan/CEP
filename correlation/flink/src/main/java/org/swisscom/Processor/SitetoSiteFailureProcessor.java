package org.swisscom.Processor;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.swisscom.POJOs.AggregationAlert_POJO;
import org.swisscom.POJOs.ZabbixEvents_POJO;
import org.swisscom.States.SitetoSiteFailureState;

import java.time.Instant;
import java.time.temporal.ChronoUnit;


public class SitetoSiteFailureProcessor extends ProcessWindowFunction<ZabbixEvents_POJO, AggregationAlert_POJO, String, TimeWindow> {

    private final static Logger logger = LoggerFactory.getLogger("SitetoSiteFailureProcessor");


    /* Count the occurrence of type of event*/
    private ValueState<SitetoSiteFailureState> countState;

    @Override
    public void open(Configuration parameters) throws Exception {
        countState = getRuntimeContext().getState(new ValueStateDescriptor<>("SiteToSiteEventValueState", SitetoSiteFailureState.class));
    }

    @Override
    public void process(String s, ProcessWindowFunction<ZabbixEvents_POJO, AggregationAlert_POJO, String, TimeWindow>.Context context, Iterable<ZabbixEvents_POJO> iterable, Collector<AggregationAlert_POJO> collector) throws Exception {

        /* Retrieve the current state */
        SitetoSiteFailureState current = this.countState.value();
        if (current == null) {
            current = new SitetoSiteFailureState();
        }
        //logger.debug("This is an debug level log message!");

        AggregationAlert_POJO aggregationAlertPojo = new AggregationAlert_POJO();

        /* Parse timestamp */
        Instant windowStart = Instant.ofEpochMilli(context.window().getStart());
        Instant windowEnd = Instant.now(); /*End of current window counting which is now*/
        windowEnd = windowEnd.truncatedTo(ChronoUnit.SECONDS);

        aggregationAlertPojo.alert_type     = "S2S_VPN";
        aggregationAlertPojo.count          = current.count;
        aggregationAlertPojo.window_start   = windowStart.toString();
        aggregationAlertPojo.window_end     = windowEnd.toString();

        collector.collect(aggregationAlertPojo);

    }

}
