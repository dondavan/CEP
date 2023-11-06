package org.swisscom.POJOs;

import java.util.ArrayList;
import java.util.HashMap;

public class TCI_POJO {
    public String zabbix_environment;
    public String event_url;
    public String zabbix_action;
    public String zabbix_site;
    public Boolean host_in_maintenance;
    public Integer trigger_id;
    public String trigger_name;
    public String trigger_description;
    public Integer event_nseverity;
    public Integer event_id;
    public String event_update_message;
    public String event_update_action;

    /* This should be Array of MAP<String,String>, save as String for now   */
    public ArrayList<HashMap<String, String>> event_tags;
    public String event_datetime;
    public String event_update_datetime;
    public String event_recovery_datetime;
    public String host_name;
    public String host_ip;
    public TCI_POJO(){}
}
