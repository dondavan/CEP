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
    public TriggerResult onElement(nqa_raw_POJO nqaRawPojo, long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        ValueState<NoConnectionFOSState> countState = triggerContext.getPartitionedState(new ValueStateDescriptor<>("NoConnectionFOSState", NoConnectionFOSState.class));
        NoConnectionFOSState current = countState.value();
        if (current == null) {
            current = new NoConnectionFOSState();
        }

        if(nqaRawPojo.metrictype.equals("FOSNqaProbe") && nqaRawPojo.value == 0) {
            Instant instant = Instant.ofEpochMilli( nqaRawPojo.timestamp ) ;
            if(instant.isAfter(Instant.now().minus(24 , ChronoUnit.HOURS))){
                current.count++;
            }
        }
        // write the state back
        countState.update(current);
        if(Event_trigger(current.count)) return TriggerResult.FIRE_AND_PURGE;
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        return TriggerResult.FIRE_AND_PURGE;
    }

    @Override
    public TriggerResult onEventTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {

    }

    private boolean Event_trigger(long count){

        /* Policy to be decided */
        return true;

    }
}
