package org.swisscom.Processor;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.swisscom.POJOs.Zabbix_events_POJO;
import org.swisscom.States.SitetoSiteFailureState;

import java.time.Instant;
import java.time.temporal.ChronoUnit;


public class SitetoSiteFailureTrigger extends Trigger<Zabbix_events_POJO, TimeWindow> {
    @Override
    /* Called each time when an element that is added to a window. */
    public TriggerResult onElement(Zabbix_events_POJO zabbixEventsPojo, long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        ValueState<SitetoSiteFailureState> countState = triggerContext.getPartitionedState(new ValueStateDescriptor<>("SiteToSiteState", SitetoSiteFailureState.class));
        SitetoSiteFailureState current = countState.value();
        if (current == null) {
            current = new SitetoSiteFailureState();
        }

        Instant instant = Instant.parse( zabbixEventsPojo.action_datetime );
        boolean fire = false;

        /*
        1.Check for alerts that are closed within the timeframe,and consider filtering them out. The alert is a non-issue,
        and it is likely just bad network conditions not a failure
        2.Check for alerts that have been open for over a day, and filter these out too. The alert is a long-standing failure, likely because a customer has disconnected their site, not a system failure.
        3.Determine the threshold based on previous data instead of on a static threshold
         */
        /* Apply Filter Policy*/
        if(     !zabbixEventsPojo.zabbix_action.equals("close") &&
                zabbixEventsPojo.trigger_name.matches(".*S2S.*") &&
                instant.isAfter(Instant.now().minus(24 , ChronoUnit.HOURS)))
        {
            current.count++;
            if(Event_trigger(current.count)) fire = true;
        }

        countState.update(current); // write the state back
        if(fire)return TriggerResult.FIRE_AND_PURGE;
        return TriggerResult.CONTINUE;
    }

    @Override
    /* Called when a registered processing-time timer fires.
    *   Processing time refers to the system time of the machine that is executing the respective operation.
    *  */
    public TriggerResult onProcessingTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    /* Called when a registered event-time timer fires.
    *   Event time is the time that each individual event occurred on its producing device
    * */
    public TriggerResult onEventTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {

    }

    private boolean Event_trigger(long count){

        /* Policy to be decided */

        return count>=5;

    }
}
