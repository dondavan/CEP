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
import org.swisscom.POJOs.FilteredEvent_POJO;
import org.swisscom.POJOs.ServiceMonitoringOutput_POJO;
import org.swisscom.POJOs.TCI_POJO;
import org.swisscom.POJOs.ZabbixEvents_POJO;
import org.swisscom.Processor.FilteringZabbixPreProcessor;
import org.swisscom.Processor.ServiceMonitoringProcessor;
import org.swisscom.Processor.StreamFilteringProcessor;

import java.time.Duration;
import java.util.Properties;

public class ZabbixTCIJointPipeline {
    private final Properties pros;

    private final StreamExecutionEnvironment env;

    /* Consumer group ID */
    String groupID = "Test-group";

    /* List of topic subscribed to  */
    private final String source_topic = "TCI";

    public ZabbixTCIJointPipeline(Properties pros, String groupID, StreamExecutionEnvironment env){
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
        JsonDeserializationSchema<TCI_POJO> TCI_JsonFormatDe=new JsonDeserializationSchema<>(TCI_POJO.class);

        /* Serializer for sending data to sink topic */
        JsonSerializationSchema<FilteredEvent_POJO> FilteredEvent_JsonFormatSe=new JsonSerializationSchema<>();


        /*****************************************************************************************

         Sink Configuration

         *****************************************************************************************/
        /* Instantiate a sink for stream processing output*/
        KafkaSink<FilteredEvent_POJO> FilteredEvent_sink = KafkaSink.<FilteredEvent_POJO>builder()
                .setBootstrapServers(this.pros.getProperty("bootstrap.servers"))

                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("FILTERED_EVENT_TOPIC")
                        .setValueSerializationSchema(FilteredEvent_JsonFormatSe)
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();


        /*****************************************************************************************

         Data Stream Configuration

         *****************************************************************************************/
        /* Instantiate a KafkaSource Instance using builder class */
        KafkaSource<TCI_POJO> TCISource = KafkaSource.<TCI_POJO >builder()

                /* Server Topic GroupID */
                .setBootstrapServers(this.pros.getProperty("bootstrap.servers"))
                .setTopics("TCI")
                .setGroupId(this.groupID)

                /* Consumer behavior */
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(TCI_JsonFormatDe)
                .build();

        KafkaSource<ZabbixEvents_POJO> ZabbixSource = KafkaSource.<ZabbixEvents_POJO>builder()

                /* Server Topic GroupID */
                .setBootstrapServers(this.pros.getProperty("bootstrap.servers"))
                .setTopics("zabbix_events")
                .setGroupId(this.groupID)

                /* Consumer behavior */
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(Zabbix_JsonFormatDe)
                .build();

        /* Get a data stream from environment through added Kafka Source*/
        DataStream<TCI_POJO> FilteringTCIStream = env.fromSource(TCISource, WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10)), "FilteringTCI_stream");
        DataStream<ZabbixEvents_POJO> FilteringZabbixStream = env.fromSource(ZabbixSource, WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10)), "FilteringZabbix_stream");

        DataStream<FilteredEvent_POJO> ServiceMonitoring_Stream =  FilteringZabbixStream.process(new FilteringZabbixPreProcessor());


        /* Transformation on data stream*/
        ServiceMonitoring_Stream.sinkTo(FilteredEvent_sink);

    }

}
