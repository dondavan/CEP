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
WITH (KAFKA_TOPIC='${ZABBIX_EVENTS_TOPIC}', VALUE_FORMAT='JSON', TIMESTAMP='`action_datetime`', TIMESTAMP_FORMAT='yyyy-MM-dd''T''HH:mm:ssZ');

CREATE SOURCE STREAM IF NOT EXISTS TCI_EVENTS_V2 (
    `zabbix_action` VARCHAR,
    `trigger_name` VARCHAR,
    `event_datetime` VARCHAR
)
WITH (KAFKA_TOPIC='${TCI_EVENTS_TOPIC}', VALUE_FORMAT='JSON', TIMESTAMP='`event_datetime`', TIMESTAMP_FORMAT='yyyy-MM-dd''T''HH:mm:ssZ');

CREATE SOURCE STREAM IF NOT EXISTS NQA_PROBES (
    `timestamp` BIGINT,
    `tenantId` VARCHAR,
    `metric` VARCHAR,
    `value` DOUBLE,
    `interval` BIGINT,
    `metrictype` VARCHAR,
    `source_ip` VARCHAR,
    `destination_ip` VARCHAR,
    `vrf` VARCHAR,
    `dc` VARCHAR
)
WITH (KAFKA_TOPIC='${ZABBIX_NQA_PROBES_TOPIC}', VALUE_FORMAT='JSON', timestamp='`timestamp`');

------ START: NODE DOWN CORRELATION
CREATE STREAM JOIN_FILTER
  WITH (KAFKA_TOPIC='${HV_FILTERED_TOPIC}', VALUE_FORMAT='JSON', TIMESTAMP='`action_datetime`', TIMESTAMP_FORMAT='yyyy-MM-dd''T''HH:mm:ss')
  AS SELECT
    ZABBIX_EVENTS_V3.`event_url` AS `event_url`,
    ZABBIX_EVENTS_V3.`zabbix_environment` AS `zabbix_environment`,
    ZABBIX_EVENTS_V3.`zabbix_site` AS `zabbix_site`,
    ZABBIX_EVENTS_V3.`host_in_maintenance` AS `host_in_maintenance`,
    ZABBIX_EVENTS_V3.`zabbix_action` AS `zabbix_action`,
    ZABBIX_EVENTS_V3.`host_name` AS `host_name`,
    ZABBIX_EVENTS_V3.`host_ip` AS `host_ip`,
    ZABBIX_EVENTS_V3.`trigger_id` AS `trigger_id`,
    ZABBIX_EVENTS_V3.`trigger_name` AS `trigger_name`,
    ZABBIX_EVENTS_V3.`trigger_description` AS `trigger_description`,
    ZABBIX_EVENTS_V3.`event_id` AS `event_id`,
    ZABBIX_EVENTS_V3.`event_tags` AS `event_tags`,
    ZABBIX_EVENTS_V3.`event_datetime` AS `event_datetime`,
    5 AS `event_nseverity`,
    ZABBIX_EVENTS_V3.`event_update_datetime` AS `event_update_datetime`,
    ZABBIX_EVENTS_V3.`event_update_message` AS `event_update_message`,
    ZABBIX_EVENTS_V3.`event_update_action` AS `event_update_action`,
    ZABBIX_EVENTS_V3.`event_recovery_datetime` AS `event_recovery_datetime`,
    ZABBIX_EVENTS_V3.`action_datetime` AS `action_datetime`,
    ROWKEY AS `synthetic_join_column`
  FROM ZABBIX_EVENTS_V3
    INNER JOIN TCI_EVENTS_V2
      WITHIN 100 MINUTES
      GRACE PERIOD 50 MINUTES
      ON COALESCE(1, LEN(ZABBIX_EVENTS_V3.`action_datetime`)) = COALESCE(1, LEN(TCI_EVENTS_V2.`event_datetime`))
  WHERE ZABBIX_EVENTS_V3.`zabbix_action` = 'create'
    AND (ZABBIX_EVENTS_V3.`trigger_name` = 'Unavailable by ZabbixProxyPortCheck' OR ZABBIX_EVENTS_V3.`event_tags`['TRIGGER'] = 'NodeDown')
    AND TCI_EVENTS_V2.`zabbix_action` = 'create'
    AND TCI_EVENTS_V2.`trigger_name` LIKE 'Zabbix agent on%compute%'
  EMIT CHANGES;
------ END: NODE DOWN CORRELATION

------ START: SERVICE MONITORING
CREATE STREAM SERVICE_MONITORING
WITH (KAFKA_TOPIC='${SERVICE_MONITORING_TOPIC}', VALUE_FORMAT='JSON', PARTITIONS=6)
 AS SELECT
  '1' as `prefix`,
  'Telco Cloud Public Service Monitoring Alert' as `token`,
  '${AGENT}' as `agent`,
  CASE
    WHEN `zabbix_action` = 'close' THEN '0'
    ELSE CAST(`event_nseverity` AS STRING)
  END as `eventSeverity`,
  '4' as `eventCategory`,
  FORMAT_TIMESTAMP(PARSE_TIMESTAMP(`action_datetime`,'yyyy-MM-dd''T''HH:mm:ss'), 'yyyy.MM.dd HH:mm:ss') as `timestamp`,
  CASE
    WHEN `trigger_name` LIKE '%High Number of ICMP%' THEN 'VMPingLossExceeded'
    WHEN `trigger_name` LIKE '%Unavailable by%' THEN 'NodeDownIWG'
    WHEN `trigger_name` LIKE '%High Number of Nodes%' THEN 'VMDownExceeded'
    WHEN `trigger_name` LIKE '%Heartbeat%' THEN 'Heartbeat'
  END as `platform`,
  '' as `nodeName`,
  'service' as `subElementType`,
  `host_name` as `subElementName`,
  '' as `snmpIndex`,
  CONCAT_WS(' ', `host_name`, `trigger_name`) as "eventText",
  '' as `circuitId`,
  '' as `circuitCode`,
  '' as `orionUrl`,
  'PIO' as `eventCat1`,
  'Cloud Services' as `eventCat2`,
  'Business Network Services' as `eventCat3`,
  '' as `eventLifeTime`,
  '' as `objectAddOn`,
  COALESCE(`event_tags`['ONDUTY'], '{EVENT.TAGS.ONDUTY}') as `onDuty`
FROM ZABBIX_EVENTS_V3
WHERE
  (`trigger_name` LIKE '%Heartbeat%')
  OR (`trigger_name` LIKE '%High Number of Nodes in %down%')
  OR (`trigger_name` LIKE '%High Number of ICMP ping loss%')
  OR ((`trigger_name` LIKE '%Unavailable by ICMPping%') AND (`event_tags`['VNF-Type'] = 'hevpe'));
------ END: SERVICE MONITORING

------ START: S2S TUNNEL DOWN
CREATE TABLE S2S_WINDOW_TABLE_TEST
  WITH (kafka_topic='${AGGREGATION_ALERTS_TOPIC}', partitions=6, value_format='JSON') AS
  SELECT
    'S2S_VPN' as `alert_type`,
    COUNT(*) as `count`,
    FORMAT_TIMESTAMP(FROM_UNIXTIME(`WINDOWSTART`),'yyyy-MM-dd''T''HH:mm:ss') AS `window_start`,
    FORMAT_TIMESTAMP(FROM_UNIXTIME(`WINDOWEND`),'yyyy-MM-dd''T''HH:mm:ss') AS `window_end`
  FROM ZABBIX_EVENTS_V3
  WINDOW TUMBLING (SIZE 5 MINUTES)
  WHERE `trigger_name` LIKE '%S2S%' AND `zabbix_action`!='close'
  GROUP BY 'S2S_VPN'
  HAVING COUNT(*) >= ${S2S_THRESHOLD}
  EMIT FINAL;
------ END: S2S TUNNEL DOWN

------ START: NQA PROBES
CREATE TABLE NQA_PROBES_CPE_WINDOW_TABLE
  WITH (kafka_topic='${AGGREGATION_ALERTS_TOPIC}', partitions=1, value_format='JSON') AS
  SELECT
    'NQA_PROBES_CPE' as `alert_type`,
    COUNT(*) as `count`,
    FORMAT_TIMESTAMP(FROM_UNIXTIME(`WINDOWSTART`),'yyyy-MM-dd''T''HH:mm:ssX') AS `window_start`,
    FORMAT_TIMESTAMP(FROM_UNIXTIME(`WINDOWEND`),'yyyy-MM-dd''T''HH:mm:ssX') AS `window_end`
  FROM NQA_PROBES
  WINDOW TUMBLING (SIZE 5 MINUTES)
  WHERE `metrictype`='CPENqaProbe' AND `value`=0
  GROUP BY 'NQA_PROBES_CPE'
  HAVING COUNT(*) >= ${NQA_PROBES_CPE_THRESHOLD}
  EMIT FINAL;

CREATE TABLE NQA_PROBES_FOS_WINDOW_TABLE
  WITH (kafka_topic='${AGGREGATION_ALERTS_TOPIC}', partitions=1, value_format='JSON') AS
  SELECT
    'NQA_PROBES_FOS' as `alert_type`,
    COUNT(*) as `count`,
    FORMAT_TIMESTAMP(FROM_UNIXTIME(`WINDOWSTART`),'yyyy-MM-dd''T''HH:mm:ssX') AS `window_start`,
    FORMAT_TIMESTAMP(FROM_UNIXTIME(`WINDOWEND`),'yyyy-MM-dd''T''HH:mm:ssX') AS `window_end`
  FROM NQA_PROBES
  WINDOW TUMBLING (SIZE 5 MINUTES)
  WHERE `metrictype`='FOSNqaProbe' AND `value`=0
  GROUP BY 'NQA_PROBES_FOS'
  HAVING COUNT(*) >= ${NQA_PROBES_FOS_THRESHOLD}
  EMIT FINAL;
------ END: NQA PROBES

------ START: NQA PROBE INTERNET FROM ZABBIX EVENTS
CREATE TABLE NQA_PROBES_INTERNET_WINDOW_TABLE
  WITH (kafka_topic='${AGGREGATION_ALERTS_TOPIC}', partitions=1, value_format='JSON') AS
  SELECT
    'NQA_PROBES_INTERNET' as `alert_type`,
    COUNT(*) as `count`,
    FORMAT_TIMESTAMP(FROM_UNIXTIME(`WINDOWSTART`),'yyyy-MM-dd''T''HH:mm:ssX') AS `window_start`,
    FORMAT_TIMESTAMP(FROM_UNIXTIME(`WINDOWEND`),'yyyy-MM-dd''T''HH:mm:ssX') AS `window_end`
  FROM ZABBIX_EVENTS_V3
  WINDOW TUMBLING (SIZE 20 MINUTES) 
  WHERE `trigger_name` LIKE 'Internet NQA Target not reachable from%' AND `zabbix_action`='create'
  GROUP BY 'NQA_PROBES_INTERNET'
  HAVING COUNT(*) >= ${NQA_PROBES_INTERNET_THRESHOLD}
  EMIT FINAL;
------ END: NQA PROBE INTERNET FROM ZABBIX EVENTS

------ START: FMG PAIR DOWN FROM ZABBIX EVENTS
--OLD 
--	htcp-lss-fmg-01
--	htcp-zhh-fmg-01
--	htcp-*-fmg-01
--	
--NEW
--	htcp-lss150-fmg-01
--	htcp-lss150-fmg-02
--	htcp-*-fmg-0*
CREATE TABLE FMG_PAIR_DOWN_WINDOW_TABLE
  WITH (kafka_topic='${AGGREGATION_ALERTS_TOPIC}', partitions=1, value_format='JSON') AS
SELECT 
  CONCAT(
  'FMG pair down *',
  CASE
    WHEN (LEN(`host_name`) = 15) THEN SUBSTRING(`host_name`, 10)
    WHEN (LEN(`host_name`) = 18) THEN SUBSTRING(`host_name`, 6, 6)
    ELSE 'NULL'
  END) `alert_type`,
COUNT_DISTINCT(`host_name`) as `count`,
FORMAT_TIMESTAMP(FROM_UNIXTIME(`WINDOWSTART`),'yyyy-MM-dd''T''HH:mm:ssX') AS `window_start`,
FORMAT_TIMESTAMP(FROM_UNIXTIME(`WINDOWEND`),'yyyy-MM-dd''T''HH:mm:ssX') AS `window_end`
FROM ZABBIX_EVENTS_V3
WINDOW SESSION (12 HOURS)
WHERE `trigger_name` LIKE '%Unavailable by ICMPping%' AND `event_tags`['VNF-Type'] = 'vfw-mgmt' AND `zabbix_action`='create'
GROUP BY
  CONCAT(
  'FMG pair down *',
  CASE
    WHEN (LEN(`host_name`) = 15) THEN SUBSTRING(`host_name`, 10)
    WHEN (LEN(`host_name`) = 18) THEN SUBSTRING(`host_name`, 6, 6)
    ELSE 'NULL'
  END) 
HAVING COUNT_DISTINCT(`host_name`) > 1
EMIT CHANGES;
------ END: FMG PAIR DOWN FROM ZABBIX EVENTS