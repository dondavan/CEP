package org.swisscom.Processor;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.swisscom.POJOs.SitetoSiteFailure_POJO;
import org.swisscom.POJOs.Zabbix_events_POJO;
import org.swisscom.States.SitetoSiteFailureState;


public class SitetoSiteFailureProcessor extends ProcessWindowFunction<Zabbix_events_POJO, SitetoSiteFailure_POJO, String, TimeWindow> {

    /* Count the occurrence of type of event*/
    private ValueState<SitetoSiteFailureState> countState;

    @Override
    public void open(Configuration parameters) throws Exception {
        countState = getRuntimeContext().getState(new ValueStateDescriptor<>("siteToSiteState", SitetoSiteFailureState.class));
    }

    @Override
    public void process(String s, ProcessWindowFunction<Zabbix_events_POJO, SitetoSiteFailure_POJO, String, TimeWindow>.Context context, Iterable<Zabbix_events_POJO> iterable, Collector<SitetoSiteFailure_POJO> collector) throws Exception {
        /*
        1.Check for alerts that are closed within the timeframe,and consider filtering them out. The alert is a non-issue,
        and it is likely just bad network conditions not a failure
        2.Check for alerts that have been open for over a day, and filter these out too. The alert is a long-standing failure, likely because a customer has disconnected their site, not a system failure.
        3.Determine the threshold based on previous data instead of on a static threshold
         */

        // retrieve the current state
        SitetoSiteFailureState current = this.countState.value();
        long count = 0;
        if (current == null) {
            current = new SitetoSiteFailureState();
            System.out.println("999");
        }
        // Get event from window iterator
        for (Zabbix_events_POJO zabbixEventsPojo: iterable) {
            System.out.println(zabbixEventsPojo);
            // retrieve the current count
            count ++;
            /* Filtering out closed event, by not reporting  */
            if(zabbixEventsPojo.zabbix_action.equals("create")){
                current.count++;
            }
            System.out.println(current.count);

            // write the state back
            countState.update(current);
            System.out.println(count);

            /* Report event and state processing */
        }

    }
}
