CREATE SOURCE STREAM IF NOT EXISTS ZABBIX_EVENTS_V3 (
    `event_url` VARCHAR,
    `zabbix_environment` VARCHAR,
    `zabbix_site` VARCHAR,
    `zabbix_action` VARCHAR,
    `host_in_maintenance` BOOLEAN,
    `host_name` VARCHAR,
    `host_ip` VARCHAR,
    `trigger_id` INT,
    `trigger_name` VARCHAR,
    `trigger_description` VARCHAR,
    `event_id` INT,
    `event_tags` MAP<VARCHAR, VARCHAR>,
    `event_datetime` VARCHAR,
    `event_nseverity` INT,
    `event_update_datetime` VARCHAR,
    `event_update_message` VARCHAR,
    `event_update_action` VARCHAR,
    `event_recovery_datetime` VARCHAR,
    `action_datetime` VARCHAR
)
WITH (KAFKA_TOPIC='${ZABBIX_EVENTS_TOPIC}', VALUE_FORMAT='JSON');
DROP STREAM IF EXISTS ZABBIX_EVENTS_V1;

CREATE SOURCE STREAM IF NOT EXISTS ZABBIX_EVENTS_V1 (
    `event_url` VARCHAR,
    `zabbix_environment` VARCHAR,
    `zabbix_site` VARCHAR,
    `zabbix_action` VARCHAR,
    `host_in_maintenance` BOOLEAN,
    `host_name` VARCHAR,
    `host_ip` VARCHAR,
    `trigger_id` INT,
    `trigger_name` VARCHAR,
    `trigger_description` VARCHAR,
    `event_id` INT,
    `event_tags` MAP<VARCHAR, VARCHAR>,
    `event_datetime` VARCHAR,
    `event_nseverity` INT,
    `event_update_datetime` VARCHAR,
    `event_update_message` VARCHAR,
    `event_update_action` VARCHAR,
    `event_recovery_datetime` VARCHAR,
    `action_datetime` VARCHAR
)
WITH (KAFKA_TOPIC='${ZABBIX_EVENTS_TOPIC}', VALUE_FORMAT='JSON');


CREATE SOURCE STREAM IF NOT EXISTS TCI_V1 (
    `zabbix_environment` VARCHAR,
    `event_url` VARCHAR,
    `zabbix_action` VARCHAR,
    `zabbix_site` VARCHAR,
    `host_in_maintenance` BOOLEAN,
    `trigger_id` INT,
    `trigger_name` VARCHAR,
    `trigger_description` VARCHAR,
    `event_nseverity` INT,
    `event_id` INT,
    `event_update_message` VARCHAR,
    `event_update_action` VARCHAR,
    `event_tags` ARRAY<MAP<VARCHAR, VARCHAR>>,
    `event_datetime` VARCHAR,
    `event_update_datetime` VARCHAR,
    `event_recovery_datetime` VARCHAR,
    `host_name` VARCHAR,
    `host_ip` VARCHAR
)
WITH (KAFKA_TOPIC='TCI', VALUE_FORMAT='JSON');

CREATE SOURCE STREAM IF NOT EXISTS NQA_RAW_V1 (
    "interval" INT,
    "metrictype" VARCHAR,
    "metric" VARCHAR,
    "tenantId" VARCHAR,
    "destination_ip" VARCHAR,
    "vrf" VARCHAR,
    "source_ip" VARCHAR,
    "dc" VARCHAR,
    "timestamp" INT,
    "value" INT
)
WITH (KAFKA_TOPIC='nqa_raw', VALUE_FORMAT='JSON');

CREATE SOURCE STREAM IF NOT EXISTS TCI_EVENTS_V3 (
    `zabbix_action` VARCHAR,
    `trigger_name` VARCHAR,
    `event_datetime` VARCHAR
)
WITH (KAFKA_TOPIC='${TCI_EVENTS_TOPIC}', VALUE_FORMAT='JSON', TIMESTAMP='`event_datetime`', TIMESTAMP_FORMAT='yyyy-MM-dd''T''HH:mm:ss');
