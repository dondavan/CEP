package org.swisscom.Pipeline;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.formats.json.JsonSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.swisscom.POJOs.ServiceMonitoringOutput_POJO;
import org.swisscom.POJOs.AggregationAlert_POJO;
import org.swisscom.POJOs.ZabbixEvents_POJO;
import org.swisscom.Processor.*;

import java.time.Duration;
import java.util.Properties;

public class ZabbixPipeline {
    private final Properties pros;

    private final StreamExecutionEnvironment env;

    /* Consumer group ID */
    String groupID = "Test-group";

    /* List of topic subscribed to  */
    private final String source_topic = "zabbix_events";

    public ZabbixPipeline(Properties pros, String groupID, StreamExecutionEnvironment env){
        this.pros = pros;
        this.groupID = groupID;
        this.env = env;
        System.out.println(source_topic);
    }

    public void createKafkaSource() {

        /* Setup separate source and sink for different topic, for different deserializer, operator */
        /*****************************************************************************************

         Serialization Configuration

         *****************************************************************************************/
        /* Json Deserializer for data from Kafka Topic */
        JsonDeserializationSchema<ZabbixEvents_POJO> Zabbix_JsonFormatDe=new JsonDeserializationSchema<>(ZabbixEvents_POJO.class);

        /* Serializer for sending data to sink topic */
        JsonSerializationSchema<ServiceMonitoringOutput_POJO> ServiceMonitoring_JsonFormatSe=new JsonSerializationSchema<>();
        JsonSerializationSchema<AggregationAlert_POJO> Aggregation_Alert_JsonFormatSe=new JsonSerializationSchema<>();


        /*****************************************************************************************

         Sink Configuration

         *****************************************************************************************/
        /* Instantiate a sink for ZabbixPipeline output*/
        KafkaSink<ServiceMonitoringOutput_POJO> ServiceMonitoring_Sink = KafkaSink.<ServiceMonitoringOutput_POJO>builder()
                .setBootstrapServers(this.pros.getProperty("bootstrap.servers"))
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("SERVICE_MONITORING_TOPIC")
                        .setValueSerializationSchema(ServiceMonitoring_JsonFormatSe)
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        KafkaSink<AggregationAlert_POJO> SitetoSiteFailure_Sink = KafkaSink.<AggregationAlert_POJO>builder()
                .setBootstrapServers(this.pros.getProperty("bootstrap.servers"))
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("aggregation-alerts")
                        .setValueSerializationSchema(Aggregation_Alert_JsonFormatSe)
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        KafkaSink<AggregationAlert_POJO> NoConnectionInternet_Sink = KafkaSink.<AggregationAlert_POJO>builder()
                .setBootstrapServers(this.pros.getProperty("bootstrap.servers"))
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("aggregation-alerts")
                        .setValueSerializationSchema(Aggregation_Alert_JsonFormatSe)
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();


        /*****************************************************************************************

         Data Stream Configuration

         *****************************************************************************************/
        /* Instantiate a KafkaSource Instance using builder class */
        KafkaSource<ZabbixEvents_POJO> kafkaSource = KafkaSource.<ZabbixEvents_POJO>builder()

                /* Server Topic GroupID */
                .setBootstrapServers(this.pros.getProperty("bootstrap.servers"))
                .setTopics(source_topic)
                .setGroupId(this.groupID)

                /* Consumer behavior */
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(Zabbix_JsonFormatDe)
                .build();

        /* Get a data stream from environment through added Kafka Source*/
        DataStream<ZabbixEvents_POJO> ServiceMonitoringStream = env.fromSource(kafkaSource, WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10)), "ServiceMonitoring_stream");
        DataStream<ZabbixEvents_POJO> S2SStream = env.fromSource(kafkaSource, WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10)), "S2S_stream");
        DataStream<ZabbixEvents_POJO> NoInternetStream = env.fromSource(kafkaSource, WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10)), "NoInternet_stream");


        /* Service Monitoring Stream Processing */
        DataStream<ServiceMonitoringOutput_POJO> ServiceMonitoring_Stream =  ServiceMonitoringStream.process(new ServiceMonitoringProcessor());

        /* Site to Site Failure Aggregation Stream Processing */
        DataStream<AggregationAlert_POJO> SitetoSiteFailure_Stream =   S2SStream
                .keyBy(value -> value.zabbix_environment)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(300)))
                .trigger(new SitetoSiteFailureTrigger())
                .process(new SitetoSiteFailureProcessor());

        /* No Connection Internet Aggregation Stream Processing */
        DataStream<AggregationAlert_POJO>  NoConnectionInternet_Stream =   NoInternetStream
                .keyBy(value -> value.zabbix_environment)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(300)))
                .trigger(new NoConnectionInternetTrigger())
                .process(new NoConnectionInternetProcessor());


        ServiceMonitoring_Stream.sinkTo(ServiceMonitoring_Sink);
        SitetoSiteFailure_Stream.sinkTo(SitetoSiteFailure_Sink);
        NoConnectionInternet_Stream.sinkTo(NoConnectionInternet_Sink);

    }
}
