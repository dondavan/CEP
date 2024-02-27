package org.swisscom.Processor;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.swisscom.POJOs.nqa_raw_POJO;
import org.swisscom.States.NoConnectionCPEState;
import org.swisscom.States.TriggerState;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

public class NoConnectionCPETrigger extends Trigger<nqa_raw_POJO, TimeWindow> {

    @Override
    /* Called each time when an element that is added to a window. */
    public TriggerResult onElement(nqa_raw_POJO nqaRawPojo, long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {

        /* Get event counting from Flink state */
        ValueState<NoConnectionCPEState> eventValueState = triggerContext.getPartitionedState(new ValueStateDescriptor<>("NoConnectionCPEEventValueState", NoConnectionCPEState.class));
        ValueState<TriggerState>       triggerValueState = triggerContext.getPartitionedState(new ValueStateDescriptor<>("NoConnectionCPETriggerValueState", TriggerState.class));

        NoConnectionCPEState eventState = eventValueState.value();
        TriggerState       triggerState = triggerValueState.value();

        if(eventState == null)     eventState  = new NoConnectionCPEState();
        if(triggerState == null) triggerState  = new TriggerState();

        Instant instant = Instant.ofEpochMilli( nqaRawPojo.timestamp );
        boolean fire = false;

        /* Apply Filter Policy*/
        if(     nqaRawPojo.metrictype.equals("CPENqaProbe") &&
                nqaRawPojo.value == 0 &&
                instant.isAfter(Instant.now().minus(24 , ChronoUnit.HOURS)))
        {
            eventState.count++;
            triggerState.count++;
            if(Event_trigger(eventState.count,triggerState.count)) fire = true;
        }

        /* write the state back */
        eventValueState.update(eventState);
        triggerValueState.update(triggerState);

        if(fire)return TriggerResult.FIRE_AND_PURGE;
        return TriggerResult.CONTINUE;
    }

    @Override
    /* Called when a registered processing-time timer fires.
    *   Processing time refers to the system time of the machine that is executing the respective operation.
    * */
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

    private boolean Event_trigger(long eventCount,long triggerCount){

        /* Policy to be decided */
        return eventCount>=5 && triggerCount==5;

    }
}
