Intern Project - Event Processing
Created by Parker David, SUB-SDC-NL-MBM-BNC, last modified by Xu Hang, SUB-SDC-NL-DOS-IIP on Feb 19, 2024
A rough outline of how the topics map to each other like may be found in the below:



The goal is to explore various technologies in order to better perform the transformations of events.

The exact mapping that exists presently may be seen here: in the source code. 



Architecture
Overview
This project `./correlation` has a legacy KSQL implemenation and a Flink implemenation.

Current flink implementaiton is based on legacy KSQL implementation, it consumes data from 3 kafka event topics `Zabbix_events`, `TCI`, `nqa_raw` and produce into 3 kafka alert topics `FILTERED_EVENT_TOPIC`, `SERVICE_MONITORING_TOPIC`, `AGGREGATION_ALERTS_TOPIC`.

Data path is abstrated as a pipeline (implemented using data stream), and each data correlation business logic is abstracted as a service to be used inside related pipeline (implmented using data stream), has an overall architecture:



Zabbix Pipeline
NQA Pipeline
TCI Zabbix Joint Pipeline
Srteam Filtering
Service Monitoring
Site-to-site
Failure
No-connection
Internet
No-connection
CPE
No-connection
FOS
TCI
NQA
Zabbix
Filtered
Events
Aggregate
Alerts
Service Monitoring
Events

Inside Pipeline
Example shows the detail of `Zabbix` pipeline, which contains correlation logic with `Zabbix_events` data, Service Monitoring, Site-to-Site Failure Aggregation, No-Connection Internet Aggregation.

A process pipeline starts with a kafka source and follows by multiple chains of data stream and kafka sink. 

Where each data stream is concrete data stream processing logic, contains features like processor function, trigger, state processing and window processing.

Kafka source and kafka sinks are configured with corresponding concrete topic following business logic.

 In this case, kafka source is configured to consume from topic `Zabbix_events`, Service Monitoring Sink is configured to produce to topic `SERVICE_MONITORING_TOPIC`

While Site-to-Site Failure Sink and No-Connection Internet Sink both produce to `AGGREGATION_ALERTS_TOPIC`.



Kafka Source
Service Monitoring
Processor
Site to Site Failure
Processor
No connection
Processor
XXX 1
Processor
XXX 2
Processor
XXX
Sink
No connect Internet
Sink
Site to Site Failure
Sink
Service Monitoring
Sink
Service Monitoring
Trigger
Site to Site
Trigger
No connection Trigger

Window and State Processing
Following diagram depicts an example of window processing on a kafka stream.

Window
Data Stream
new event
*** : ***
action_datetime : ***
Trigger
onElement
onProcessingTime
onEventTime
State
Window Result



Correlation Logic Design
Filtering Event
Event Filtering is performed base on Zabbix Event and TCI event.

 

Filtering
Event
Topic
Timeline
TCI
Zabbix
Filtering
Event
Trigger
Trigger
Trigger
TCI trigger base on Event Date Time? or Arriving Time? This will influence if filtering will include previous-1 zabbix event

Aggregation
Example Use Case
Site to Site Aggregation
Here will give a demonstration of Site to Site aggregation.

Site to Site performs aggregation logic of:

"zabbix_action" != "close"
"trigger_name" matches regx ".*S2S.*"
"action_datetime" is within last 24 hours
With a tumbling window size of 5 minutes, performing incremental aggregation(on arrival of each event) based on wall clock.

It will count eligible event arrived within window, only fire a alert when trigger policy is met (Currently count >= 5) with transformed field value, event count and window metadata.

1
1
2
2
Site to Site Aggregation





The current project steps are as follows:
Establish a data generation project
Replicate the existing ksqldb implementation
Implement existing logic using flink
Improve logic using flink and flink CEP
Transformations are listed below in order of complexity.

Service-monitoring
Service Monitoring transformation is represented by a simple mapping of fields, and is probably the best place to start in order to have a working transformation pipeline.

Site-to-site
Site to site currently exists as a count of all 'create' events within a period of time. If this goes above a static threshold, an alert is triggered.

We aim to add some flexibility with flink. Some ideas:

check for alerts that are closed within the timeframe, and consider filtering them out. The alert is a non-issue, and it is likely just bad network conditions not a failure
check for alerts that have been open for over a day, and filter these out too. The alert is a long-standing failure, likely because a customer has disconnected their site, not a system failure.
determine the threshold based on previous data instead of on a static threshold
This would be ideal if implemented in a somewhat generic way, as it is also applicable to the Network quality assessment for  internet, CPE and FOS.



Filtering
this use-case considers filtering events based on the primary events topic ("zabbix events") based on presence of a event in the secondary ("Hypervisor events"). At present, a stream inner join is done, which does a join on a dummy field between two topics within a time window. The result is basically a cross-product of the two topics, resulting in massive duplication in the case of multiple events in the HV topic. 

We could look to formulate this differently as a single stream, and consider a pattern of events instead of a join, and emit the results, thereby removing duplicates. Or by finding some other way to abstract the idea in such a way that there is not massive duplication. 

The method here also does not permit the passing of 'close' events that come much later than 'create' events. A secondary goal should be to find a way to also pass on the 'close' events for the events that are produced.