package org.swisscom.Processor;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.swisscom.POJOs.nqa_raw_POJO;
import org.swisscom.States.NoConnectionCPEState;
import org.swisscom.States.NoConnectionFOSState;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

public class NoConnectionFOSTrigger extends Trigger<nqa_raw_POJO, TimeWindow> {
    @Override
    /* Called each time when an element that is added to a window. */
    public TriggerResult onElement(nqa_raw_POJO nqaRawPojo, long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {

        /* Get event counting from Flink state */
        ValueState<NoConnectionFOSState> countState = triggerContext.getPartitionedState(new ValueStateDescriptor<>("NoConnectionFOSState", NoConnectionFOSState.class));
        NoConnectionFOSState current = countState.value();
        if (current == null) {
            current = new NoConnectionFOSState();
        }

        Instant instant = Instant.ofEpochMilli( nqaRawPojo.timestamp );
        boolean fire = false;

        /* Apply Filter Policy*/
        if(     nqaRawPojo.metrictype.equals("FOSNqaProbe") &&
                nqaRawPojo.value == 0 &&
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

    private boolean Event_trigger(long count){

        /* Policy to be decided */
        return count>5;

    }
}
