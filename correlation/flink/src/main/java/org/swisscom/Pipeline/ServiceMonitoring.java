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
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.swisscom.POJOs.ServiceMonitoringOutput_POJO;
import org.swisscom.POJOs.Aggregation_Alert_POJO;
import org.swisscom.POJOs.Zabbix_events_POJO;
import org.swisscom.Processor.ServiceMonitoringProcessor;
import org.swisscom.Processor.SitetoSiteFailureProcessor;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

public class ServiceMonitoring {
    private final Properties pros;

    /* List of topic subscribed to  */
    private final List<String> topics;

    private final StreamExecutionEnvironment env;

    /* Consumer group ID */
    String groupID = "Test-group";

    public ServiceMonitoring(Properties pros, Collection<String> topics, String groupID, StreamExecutionEnvironment env){
        this.pros = pros;
        this.topics = new ArrayList<String>(topics);
        this.groupID = groupID;
        this.env = env;
        System.out.println(topics);
    }

    public void createKafkaSource() {

        /* Setup separate source and sink for different topic, for different deserializer, operator */
        for (String topic:topics) {
            /*****************************************************************************************

                                            Serialization Area

             *****************************************************************************************/
            /* Json Deserializer and Serializer for Data from Kafka Topic */
            JsonDeserializationSchema<Zabbix_events_POJO> jsonFormatDe=new JsonDeserializationSchema<>(Zabbix_events_POJO.class);
            JsonSerializationSchema<ServiceMonitoringOutput_POJO> ServiceMonitoring_JsonFormatSe=new JsonSerializationSchema<>();
            JsonSerializationSchema<Aggregation_Alert_POJO> SitetoSiteFailure_JsonFormatSe=new JsonSerializationSchema<>();


            /*****************************************************************************************

             Sink Area

             *****************************************************************************************/
            /* Instantiate a sink for ServiceMonitoring output*/
            KafkaSink<ServiceMonitoringOutput_POJO> ServiceMonitoring_Sink = KafkaSink.<ServiceMonitoringOutput_POJO>builder()
                    .setBootstrapServers(this.pros.getProperty("bootstrap.servers"))
                    .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                            .setTopic("ServiceMonitoring_sink")
                            .setValueSerializationSchema(ServiceMonitoring_JsonFormatSe)
                            .build()
                    )
                    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .build();

            KafkaSink<Aggregation_Alert_POJO> SitetoSiteFailure_Sink = KafkaSink.<Aggregation_Alert_POJO>builder()
                    .setBootstrapServers(this.pros.getProperty("bootstrap.servers"))
                    .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                            .setTopic("AGGREGATION_ALERTS_TOPIC")
                            .setValueSerializationSchema(SitetoSiteFailure_JsonFormatSe)
                            .build()
                    )
                    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .build();


            /*****************************************************************************************

                                            Data Stream Area

             *****************************************************************************************/
            /* Instantiate a KafkaSource Instance using builder class */
            KafkaSource<Zabbix_events_POJO> kafkaSource = KafkaSource.<Zabbix_events_POJO>builder()

                    /* Server Topic GroupID */
                    .setBootstrapServers(this.pros.getProperty("bootstrap.servers"))
                    .setTopics(topic)
                    .setGroupId(this.groupID)

                    /* Consumer behavior */
                    .setStartingOffsets(OffsetsInitializer.earliest())
                    .setValueOnlyDeserializer(jsonFormatDe)
                    .build();

            /* Get a data stream from environment through added Kafka Source*/
            DataStream<Zabbix_events_POJO> kafkaStream = env.fromSource(kafkaSource, WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(20)), topic+"_stream");

            /* Create a data stream to process transformation*/
            DataStream<ServiceMonitoringOutput_POJO> ServiceMonitoring_Stream =  kafkaStream.process(new ServiceMonitoringProcessor()).startNewChain();
            ServiceMonitoring_Stream.sinkTo(ServiceMonitoring_Sink);

            /* Create a Site to Site Failure transformation data stream */
            DataStream<Aggregation_Alert_POJO> SitetoSiteFailure_Stream = kafkaStream
                                                                            .keyBy(value -> value.zabbix_environment)
                                                                            .window(TumblingProcessingTimeWindows.of(Time.seconds(60)))
                                                                            .process(new SitetoSiteFailureProcessor());

            SitetoSiteFailure_Stream.sinkTo(SitetoSiteFailure_Sink);

        }
    }
}
