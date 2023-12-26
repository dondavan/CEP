package org.swisscom.Processor;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.swisscom.POJOs.Aggregation_Alert_POJO;
import org.swisscom.POJOs.Zabbix_events_POJO;
import org.swisscom.States.SitetoSiteFailureState;

import java.time.Instant;
import java.time.temporal.ChronoUnit;


public class SitetoSiteFailureProcessorReduce extends RichReduceFunction<Zabbix_events_POJO> {

    /* Count the occurrence of type of event*/
    private ValueState<SitetoSiteFailureState> countState;

    @Override
    public void open(Configuration parameters) throws Exception {
        countState = getRuntimeContext().getState(new ValueStateDescriptor<>("siteToSiteState", SitetoSiteFailureState.class));
    }

    @Override
    public Zabbix_events_POJO reduce(Zabbix_events_POJO zabbixEventsPojo, Zabbix_events_POJO t1) throws Exception {
        // retrieve the current state
        SitetoSiteFailureState current = this.countState.value();
        if (current == null) {
            current = new SitetoSiteFailureState();
        }
        if(zabbixEventsPojo.zabbix_action.equals("create")) {
            Instant instant = Instant.parse( zabbixEventsPojo.action_datetime ) ;
            if(instant.isAfter(Instant.now().minus(24 , ChronoUnit.HOURS))){
                current.count++;
            }
        }
        // write the state back
        countState.update(current);
        return null;
    }
}
