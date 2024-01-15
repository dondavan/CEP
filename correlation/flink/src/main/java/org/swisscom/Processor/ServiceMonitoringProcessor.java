package org.swisscom.Processor;

import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.swisscom.POJOs.ServiceMonitoringOutput_POJO;
import org.swisscom.POJOs.Zabbix_events_POJO;

import java.lang.reflect.InvocationTargetException;

public class ServiceMonitoringProcessor extends ProcessFunction<Zabbix_events_POJO,ServiceMonitoringOutput_POJO> {


    @Override
    public void processElement(Zabbix_events_POJO zabbixEvent, ProcessFunction<Zabbix_events_POJO, ServiceMonitoringOutput_POJO>.Context context, Collector<ServiceMonitoringOutput_POJO> collector) throws Exception {

        /* Event trigger through String matching*/
        if (Event_trigger(zabbixEvent.trigger_name,zabbixEvent.event_tags.get("VNF-Type"))){
            ServiceMonitoringOutput_POJO serviceMonitoringOutputPojo = new ServiceMonitoringOutput_POJO();

            serviceMonitoringOutputPojo.prefix          = "1";
            serviceMonitoringOutputPojo.token           = "Telco Cloud Public Service Monitoring Alert";
            serviceMonitoringOutputPojo.agent           = "${AGENT}";
            serviceMonitoringOutputPojo.eventCategory   = "4";
            serviceMonitoringOutputPojo.nodeName        = "";
            serviceMonitoringOutputPojo.subElementType  = "service";
            serviceMonitoringOutputPojo.subElementName  = zabbixEvent.host_name;
            serviceMonitoringOutputPojo.snmpIndex       = "";
            serviceMonitoringOutputPojo.circuitId       = "";
            serviceMonitoringOutputPojo.circuitCode     = "";
            serviceMonitoringOutputPojo.orionUrl        = "";
            serviceMonitoringOutputPojo.eventCat1       = "PIO";
            serviceMonitoringOutputPojo.eventCat2       = "Cloud Services";
            serviceMonitoringOutputPojo.eventCat3       = "Business Network Services";
            serviceMonitoringOutputPojo.eventLifeTime   = "";
            serviceMonitoringOutputPojo.objectAddOn     = "";


            if(zabbixEvent.zabbix_action.matches("close")){
                serviceMonitoringOutputPojo.eventSeverity = "0";
            }else {
                serviceMonitoringOutputPojo.eventSeverity = zabbixEvent.event_nseverity.toString();
            }

            /* Format 'yyyy-MM-dd''T''HH:mm:ssX'  to 'yyyy.MM.dd HH:mm:ss' */
            String newdate = zabbixEvent.action_datetime.replace("-",".")
                                                        .replace("T", " ");
            newdate = newdate.substring(0,newdate.length()-1);
            serviceMonitoringOutputPojo.timestamp = newdate;


            if(zabbixEvent.trigger_name.matches("%High Number of ICMP%")){
                serviceMonitoringOutputPojo.platform = "VMPingLossExceeded";
            } else if (zabbixEvent.trigger_name.matches("%Unavailable by%")){
                serviceMonitoringOutputPojo.platform = "NodeDownIWG";
            }else if (zabbixEvent.trigger_name.matches("%High Number of Nodes%")){
                serviceMonitoringOutputPojo.platform = "VMDownExceeded";
            }else if (zabbixEvent.trigger_name.matches("%Heartbeat%")){
                serviceMonitoringOutputPojo.platform = "Heartbeat";
            }else {
                serviceMonitoringOutputPojo.platform = "";
            }

            serviceMonitoringOutputPojo.eventText = zabbixEvent.host_name + " " + zabbixEvent.trigger_name;

            if(zabbixEvent.event_tags.get("ONDUTY") != null){
                serviceMonitoringOutputPojo.onDuty = zabbixEvent.event_tags.get("ONDUTY");
            }else {
                serviceMonitoringOutputPojo.onDuty = "{EVENT.TAGS.ONDUTY}";
            }
            collector.collect(serviceMonitoringOutputPojo);

        }

    }

    /* Reimplemented from original ksql query, WHERE statement*/
    private boolean Event_trigger(String trigger_name, String event_tags){

        String expression1 = "%Heartbeat%";
        String expression2 = "%High Number of Nodes in %down%";
        String expression3 = "%High Number of ICMP ping loss%";
        String expression4 = "%Unavailable by ICMPping%";
        String expression5 = "hevpe";

        return  trigger_name.matches(expression1) |
                trigger_name.matches(expression2) |
                trigger_name.matches(expression3) |
                (trigger_name.matches(expression4) & event_tags.matches(expression5));

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
