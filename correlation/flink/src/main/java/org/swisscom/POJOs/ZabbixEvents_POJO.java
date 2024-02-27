package org.swisscom.POJOs;

import java.util.HashMap;

public class ZabbixEvents_POJO {
    public String event_url;
    public String zabbix_environment;
    public String zabbix_site;
    public String zabbix_action;
    public String host_in_maintenance;
    public String host_name;
    public String host_ip;
    public Integer trigger_id;
    public String trigger_name;
    public String trigger_description;
    public Integer event_id;

    /* This should be Array of MAP<String,String>, save as String for now   */
    public HashMap<String, String> event_tags;
    public String event_datetime;
    public Integer event_nseverity;
    public String event_update_datetime;
    public String event_update_message;
    public String event_update_action;
    public String event_recovery_datetime;
    public String action_datetime;

    public ZabbixEvents_POJO(){}
}
