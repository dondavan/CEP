package org.swisscom.Processor;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.swisscom.POJOs.Aggregation_Alert_POJO;
import org.swisscom.POJOs.nqa_raw_POJO;
import org.swisscom.States.NoConnectionCPEState;
import org.swisscom.States.NoConnectionFOSState;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

public class NoConnectionFOSProcessor extends ProcessWindowFunction<nqa_raw_POJO, Aggregation_Alert_POJO, String, TimeWindow> {

    /* Count the occurrence of type of event*/
    private ValueState<NoConnectionFOSState> countState;

    @Override
    public void open(Configuration parameters) throws Exception {
        countState = getRuntimeContext().getState(new ValueStateDescriptor<>("NoConnectionFOSState", NoConnectionFOSState.class));
    }

    @Override
    public void process(String s, ProcessWindowFunction<nqa_raw_POJO, Aggregation_Alert_POJO, String, TimeWindow>.Context context, Iterable<nqa_raw_POJO> iterable, Collector<Aggregation_Alert_POJO> collector) throws Exception {

        /* Retrieve the current state */
        NoConnectionFOSState current = this.countState.value();
        if (current == null) {
            current = new NoConnectionFOSState();
        }

        Aggregation_Alert_POJO aggregationAlertPojo = new Aggregation_Alert_POJO();

        /* Parse timestamp */
        Instant windowStart = Instant.ofEpochMilli(context.window().getStart());
        Instant windowEnd = Instant.now(); /*End of current window counting which is now*/
        windowEnd = windowEnd.truncatedTo(ChronoUnit.SECONDS);

        aggregationAlertPojo.alert_type     = "NQA_PROBES_FOS";
        aggregationAlertPojo.count          = current.count;
        aggregationAlertPojo.window_start   = windowStart.toString();
        aggregationAlertPojo.window_end     = windowEnd.toString();

        collector.collect(aggregationAlertPojo);
    }
}
