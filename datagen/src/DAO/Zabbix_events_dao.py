class Zabbix_events_dao(object):
    
    schema_str = """
    {
        "$id": "http://example.com/myURI.schema.json",
        "$schema": "http://json-schema.org/draft-07/schema#",
        "additionalProperties": false,
        "description": "JSON schema for zabbix events.",
        "properties": {
            "action_datetime": {
            "type": "string"
            },
            "event_datetime": {
            "type": "string"
            },
            "event_id": {
            "type": "integer"
            },
            "event_nseverity": {
            "type": "integer"
            },
            "event_recovery_datetime": {
            "type": "string"
            },
            "event_tags": {
            "TRIGGER": {
                "type": "string"
            },
            "VNF-Type": {
                "type": "string"
            }
            },
            "event_update_action": {
            "type": "string"
            },
            "event_update_datetime": {
            "type": "string"
            },
            "event_update_message": {
            "type": "string"
            },
            "event_url": {
            "type": "string"
            },
            "host_in_maintenance": {
            "type": "boolean"
            },
            "host_ip": {
            "type": "string"
            },
            "host_name": {
            "type": "string"
            },
            "trigger_description": {
            "type": "string"
            },
            "trigger_id": {
            "type": "integer"
            },
            "trigger_name": {
            "type": "string"
            },
            "zabbix_action": {
            "type": "string"
            },
            "zabbix_environment": {
            "type": "string"
            },
            "zabbix_site": {
            "type": "string"
            }
        },
        "title": "Zabbix_events_schema",
        "type": "object"
        }
    """