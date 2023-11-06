package org.swisscom.Processor;

import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.swisscom.POJOs.Zabbix_events_POJO;

import java.lang.reflect.InvocationTargetException;

public class ServiceMonitoringProcessor extends ProcessFunction<Zabbix_events_POJO,Zabbix_events_POJO> {

    @Override
    public void processElement(Zabbix_events_POJO zabbixEvent, ProcessFunction<Zabbix_events_POJO, Zabbix_events_POJO>.Context context, Collector<Zabbix_events_POJO> collector) throws Exception {

        /* Event trigger through String matching*/
        System.out.println(zabbixEvent.event_tags.getClass());
        System.out.println(Event_trigger(zabbixEvent.trigger_name));

    }

    /* Reimplemented from original ksql query, WHERE statement*/
    private boolean Event_trigger(String trigger_name){

        String expression1 = "%Heartbeat%";
        String expression2 = "%High Number of Nodes in %down%";
        String expression3 = "%High Number of ICMP ping loss%";
        String expression4 = "%Unavailable by ICMPping%";

        return  trigger_name.matches(expression1) |
                trigger_name.matches(expression2) |
                trigger_name.matches(expression3) |
                (trigger_name.matches(expression4));

        /*
        (`trigger_name` LIKE '%Heartbeat%')
        OR (`trigger_name` LIKE '%High Number of Nodes in %down%')
        OR (`trigger_name` LIKE '%High Number of ICMP ping loss%')
        OR ((`trigger_name` LIKE '%Unavailable by ICMPping%') AND (`event_tags`['VNF-Type'] = 'hevpe'));
         */

    }

    public Object deseralizer(Object obj, String topic){

        /* Get POJO object through reflection*/
        Object POJO;
        JsonDeserializationSchema<?> jsonFormat;
        String POJO_LOCATION = "org.swisscom.POJOs."+topic+"_POJO"; /* package name + class name */
        try {
            POJO = (Object) Class.forName(POJO_LOCATION).getConstructor().newInstance();
            jsonFormat=new JsonDeserializationSchema<>(Class.forName(POJO_LOCATION));

        } catch (InstantiationException | IllegalAccessException | InvocationTargetException |
                 NoSuchMethodException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        return POJO;
    }
}
